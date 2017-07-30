package raft

import (
    "github.com/gorilla/mux"
    "github.com/coreos/etcd/raft/raftpb"
    "golang.org/x/net/context"

    "devicedb"

    "fmt"
    "errors"
    "net/http"
    "bytes"
    "io"
    "io/ioutil"
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
    raftNode *RaftNode
}

func NewTransportHub(raftNode *RaftNode) *TransportHub {
    return &TransportHub{
        peers: make(map[uint64]PeerAddress),
        httpClient: &http.Client{ },
        raftNode: raftNode,
    }
}

func (hub *TransportHub) AddPeer(peerAddress PeerAddress) {
    hub.peers[peerAddress.NodeID] = peerAddress
}

func (hub *TransportHub) RemovePeer(peerAddress PeerAddress) {
    delete(hub.peers, peerAddress.NodeID)
}

func (hub *TransportHub) UpdatePeer(peerAddress PeerAddress) {
    hub.AddPeer(peerAddress)
}

func (hub *TransportHub) Receive(ctx context.Context, msg raftpb.Message) error {
    return hub.raftNode.Receive(ctx, msg)
}

func (hub *TransportHub) Send(ctx context.Context, msg raftpb.Message) error {
    encodedMessage, err := msg.Marshal()

    if err != nil {
        return err
    }

    peerAddress, ok := hub.peers[msg.To]

    if !ok {
        return errors.New("Trying to send message to peer that is not known by the transport hub")
    }

    request, err := http.NewRequest("POST", peerAddress.ToHTTPURL("/raftmessages"), bytes.NewReader(encodedMessage))

    if err != nil {
        return err
    }

    resp, err := hub.httpClient.Do(request)
    
    if err != nil {
        return err
    }
    
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        errorMessage, err := ioutil.ReadAll(resp.Body)
        
        if err != nil {
            return err
        }
        
        return errors.New(fmt.Sprintf("Received error code from server: (%d) %s", resp.StatusCode, string(errorMessage)))
    }

    return nil
}

func (hub *TransportHub) Attach(router mux.Router) {
    router.HandleFunc("/raftmessages", func(w http.ResponseWriter, r *http.Request) {
        raftMessage, err := ioutil.ReadAll(r.Body)

        if err != nil {
            devicedb.Log.Warningf("POST /raftmessages: Unable to read message body")

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        var msg raftpb.Message

        err = msg.Unmarshal(raftMessage)

        if err != nil {
            devicedb.Log.Warningf("POST /raftmessages: Unable to parse message body")

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        err = hub.Receive(r.Context(), msg)

        if err != nil {
            devicedb.Log.Warningf("POST /raftmessages: Unable to receive message")

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