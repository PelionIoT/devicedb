package raft

import (
    "time"
    "strings"
    "github.com/gorilla/mux"
    "github.com/coreos/etcd/raft/raftpb"

    . "devicedb/logging"

    "fmt"
    "errors"
    "net/http"
    "bytes"
    "io"
    "io/ioutil"
    "context"
    "sync"
)

var ESenderUnknown = errors.New("The receiver does not know who we are")
var EReceiverUnknown = errors.New("The sender does not know the receiver")
var ETimeout = errors.New("The sender timed out while trying to send the message to the receiver")

const (
    RequestTimeoutSeconds = 10
)

type PeerAddress struct {
    NodeID uint64
    Host string
    Port int
}

func (peerAddress *PeerAddress) ToHTTPURL(endpoint string) string {
    return fmt.Sprintf("http://%s:%d%s", peerAddress.Host, peerAddress.Port, endpoint)
}

type TransportHub struct {
    peers map[uint64]PeerAddress
    httpClient *http.Client
    onReceiveCB func(context.Context, raftpb.Message) error
    lock sync.Mutex
}

func NewTransportHub() *TransportHub {
    hub := &TransportHub{
        peers: make(map[uint64]PeerAddress),
        httpClient: &http.Client{ 
            Timeout: time.Second * RequestTimeoutSeconds,
        },
    }

    return hub
}

func (hub *TransportHub) AddPeer(peerAddress PeerAddress) {
    hub.lock.Lock()
    defer hub.lock.Unlock()
    hub.peers[peerAddress.NodeID] = peerAddress
}

func (hub *TransportHub) RemovePeer(peerAddress PeerAddress) {
    hub.lock.Lock()
    defer hub.lock.Unlock()
    delete(hub.peers, peerAddress.NodeID)
}

func (hub *TransportHub) UpdatePeer(peerAddress PeerAddress) {
    hub.lock.Lock()
    defer hub.lock.Unlock()
    hub.AddPeer(peerAddress)
}

func (hub *TransportHub) OnReceive(cb func(context.Context, raftpb.Message) error) {
    hub.onReceiveCB = cb
}

func (hub *TransportHub) Send(ctx context.Context, msg raftpb.Message) error {
    encodedMessage, err := msg.Marshal()

    if err != nil {
        return err
    }

    hub.lock.Lock()
    peerAddress, ok := hub.peers[msg.To]
    hub.lock.Unlock()

    if !ok {
        return EReceiverUnknown
    }

    request, err := http.NewRequest("POST", peerAddress.ToHTTPURL("/raftmessages"), bytes.NewReader(encodedMessage))

    if err != nil {
        return err
    }

    resp, err := hub.httpClient.Do(request)
    
    if err != nil {
        if strings.Contains(err.Error(), "Timeout") {
            return ETimeout
        }

        return err
    }
    
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        if resp.StatusCode == http.StatusForbidden {
            return ESenderUnknown
        }

        errorMessage, err := ioutil.ReadAll(resp.Body)
        
        if err != nil {
            return err
        }
        
        return errors.New(fmt.Sprintf("Received error code from server: (%d) %s", resp.StatusCode, string(errorMessage)))
    }

    return nil
}

func (hub *TransportHub) Attach(router *mux.Router) {
    router.HandleFunc("/raftmessages", func(w http.ResponseWriter, r *http.Request) {
        raftMessage, err := ioutil.ReadAll(r.Body)

        if err != nil {
            Log.Warningf("POST /raftmessages: Unable to read message body")

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        var msg raftpb.Message

        err = msg.Unmarshal(raftMessage)

        if err != nil {
            Log.Warningf("POST /raftmessages: Unable to parse message body")

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        hub.lock.Lock()
        _, ok := hub.peers[msg.From]
        hub.lock.Unlock()

        if !ok {
            Log.Warningf("POST /raftmessages: Sender node (%d) is not known by this node", msg.To)

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusForbidden)
            io.WriteString(w, "\n")
            
            return
        }

        err = hub.onReceiveCB(r.Context(), msg)

        if err != nil {
            Log.Warningf("POST /raftmessages: Unable to receive message: %v", err.Error())

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")
}