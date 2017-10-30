package server

import (
    "github.com/gorilla/websocket"
    "sync"
    "errors"
    "time"
    "crypto/tls"
    crand "crypto/rand"
    "fmt"
    "encoding/binary"
    "encoding/json"
    "strconv"
    "net/http"
    "io/ioutil"
    "bytes"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/historian"
    . "devicedb/logging"
    ddbSync "devicedb/sync"
)

const (
    INCOMING = iota
    OUTGOING = iota
)

const SYNC_SESSION_WAIT_TIMEOUT_SECONDS = 5
const RECONNECT_WAIT_MAX_SECONDS = 32
const WRITE_WAIT_SECONDS = 10
const PONG_WAIT_SECONDS = 60
const PING_PERIOD_SECONDS = 40
const CLOUD_PEER_ID = "cloud"

func randomID() string {
    randomBytes := make([]byte, 16)
    crand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%016x%016x", high, low)
}

type PeerJSON struct {
    Direction string `json:"direction"`
    ID string `json:"id"`
    Status string `json:"status"`
}

type Peer struct {
    id string
    connection *websocket.Conn
    direction int
    closed bool
    closeChan chan bool
    doneChan chan bool
    csLock sync.Mutex
    rttLock sync.Mutex
    roundTripTime time.Duration
    result error
    host string
    port int
    historyHost string
    historyPort int
    partitionNumber uint64
    siteID string
    httpClient *http.Client
    httpHistoryClient *http.Client
    identityHeader string
}

func NewPeer(id string, direction int) *Peer {
    return &Peer{
        id: id,
        direction: direction,
        closeChan: make(chan bool, 1),
    }
}

func (peer *Peer) errors() error {
    return peer.result
}

func (peer *Peer) accept(connection *websocket.Conn) (chan *SyncMessageWrapper, chan *SyncMessageWrapper, error) {
    peer.csLock.Lock()
    defer peer.csLock.Unlock()
    
    if peer.closed {
        return nil, nil, errors.New("Peer closed")
    }
    
    peer.connection = connection
    
    incoming, outgoing := peer.establishChannels()
    
    return incoming, outgoing, nil
}

func (peer *Peer) connect(dialer *websocket.Dialer, host string, port int) (chan *SyncMessageWrapper, chan *SyncMessageWrapper, error) {
    reconnectWaitSeconds := 1
    
    peer.host = host
    peer.port = port
    peer.httpClient = &http.Client{ Transport: &http.Transport{ TLSClientConfig: dialer.TLSClientConfig } }

    var header http.Header = make(http.Header)

    if peer.identityHeader != "" {
        header.Set("X-WigWag-RelayID", peer.identityHeader)
    }
    
    for {
        peer.connection = nil

        conn, _, err := dialer.Dial("wss://" + host + ":" + strconv.Itoa(port) + "/sync", header)
                
        if err != nil {
            Log.Warningf("Unable to connect to peer %s at %s on port %d: %v. Reconnecting in %ds...", peer.id, host, port, err, reconnectWaitSeconds)
            
            select {
            case <-time.After(time.Second * time.Duration(reconnectWaitSeconds)):
            case <-peer.closeChan:
                Log.Debugf("Cancelled connection retry sequence for %s", peer.id)
                
                return nil, nil, errors.New("Peer closed")
            }
            
            if reconnectWaitSeconds != RECONNECT_WAIT_MAX_SECONDS {
                reconnectWaitSeconds *= 2
            }
        } else {
            peer.csLock.Lock()
            defer peer.csLock.Unlock()
            
            if !peer.closed {
                peer.connection = conn
                
                incoming, outgoing := peer.establishChannels()
                
                return incoming, outgoing, nil
            }
            
            Log.Debugf("Cancelled connection retry sequence for %s", peer.id)
            
            closeWSConnection(conn)
            
            return nil, nil, errors.New("Peer closed")
        }
    }
}

func (peer *Peer) establishChannels() (chan *SyncMessageWrapper, chan *SyncMessageWrapper) {
    connection := peer.connection
    peer.doneChan = make(chan bool, 1)
    
    incoming := make(chan *SyncMessageWrapper)
    outgoing := make(chan *SyncMessageWrapper)
    
    go func() {
        pingTicker := time.NewTicker(time.Second * PING_PERIOD_SECONDS)

        for {
            select {
            case msg, ok := <-outgoing:
                // this lock ensures mutual exclusion with close message sending in peer.close()
                peer.csLock.Lock()
                connection.SetWriteDeadline(time.Now().Add(time.Second * WRITE_WAIT_SECONDS))

                if !ok {
                    connection.WriteMessage(websocket.CloseMessage, []byte{})
                    peer.csLock.Unlock()
                    return
                }

                err := connection.WriteJSON(msg)
                peer.csLock.Unlock()

                if err != nil {
                    Log.Errorf("Error writing to websocket for peer %s: %v", peer.id, err)
                    //return
                }
            case <-pingTicker.C:
                // this lock ensures mutual exclusion with close message sending in peer.close()
                Log.Infof("Sending a ping to peer %s", peer.id)
                peer.csLock.Lock()
                connection.SetWriteDeadline(time.Now().Add(time.Second * WRITE_WAIT_SECONDS))

                encodedPingTime, _ := time.Now().MarshalJSON()
                if err := connection.WriteMessage(websocket.PingMessage, encodedPingTime); err != nil {
                    Log.Errorf("Unable to send ping to peer %s: %v", peer.id, err.Error())
                }

                peer.csLock.Unlock()
            }
        }
    }()
    
    // incoming, outgoing, err
    go func() {
        defer close(peer.doneChan)

        connection.SetReadDeadline(time.Now().Add(time.Second * PONG_WAIT_SECONDS))
        connection.SetPongHandler(func(encodedPingTime string) error {
            var pingTime time.Time

            if err := pingTime.UnmarshalJSON([]byte(encodedPingTime)); err == nil {
                Log.Infof("Received pong from peer %s. Round trip time: %v", peer.id, time.Since(pingTime))
                peer.setRoundTripTime(time.Since(pingTime))
            } else {
                Log.Infof("Received pong from peer %s", peer.id)
            }

            connection.SetReadDeadline(time.Now().Add(time.Second * PONG_WAIT_SECONDS));

            return nil
        })
        
        for {
            var nextRawMessage rawSyncMessageWrapper
            var nextMessage SyncMessageWrapper
            
            err := connection.ReadJSON(&nextRawMessage)
            
            if err != nil {
                if err.Error() == "websocket: close 1000 (normal)" {
                    Log.Infof("Received a normal websocket close message from peer %s", peer.id)
                } else {
                    Log.Errorf("Peer %s sent a misformatted message. Unable to parse: %v", peer.id, err)
                }
                
                peer.result = err
                
                close(incoming)
            
                return
            }
            
            nextMessage.SessionID = nextRawMessage.SessionID
            nextMessage.MessageType = nextRawMessage.MessageType
            nextMessage.Direction = nextRawMessage.Direction
            
            err = peer.typeCheck(&nextRawMessage, &nextMessage)
            
            if err != nil {
                peer.result = err
                
                close(incoming)
                
                return
            }
            
            nextMessage.nodeID = peer.id
            
            incoming <- &nextMessage
        }    
    }()
    
    return incoming, outgoing
}

func (peer *Peer) setRoundTripTime(duration time.Duration) {
    peer.rttLock.Lock()
    defer peer.rttLock.Unlock()

    peer.roundTripTime = duration
}

func (peer *Peer) getRoundTripTime() time.Duration {
    peer.rttLock.Lock()
    defer peer.rttLock.Unlock()

    return peer.roundTripTime
}

func (peer *Peer) typeCheck(rawMsg *rawSyncMessageWrapper, msg *SyncMessageWrapper) error {
    var err error = nil
    
    switch msg.MessageType {
    case SYNC_START:
        var start Start
        err = json.Unmarshal(rawMsg.MessageBody, &start)
        msg.MessageBody = start
    case SYNC_ABORT:
        var abort Abort
        err = json.Unmarshal(rawMsg.MessageBody, &abort)
        msg.MessageBody = abort
    case SYNC_NODE_HASH:
        var nodeHash MerkleNodeHash
        err = json.Unmarshal(rawMsg.MessageBody, &nodeHash)
        msg.MessageBody = nodeHash
    case SYNC_OBJECT_NEXT:
        var objectNext ObjectNext
        err = json.Unmarshal(rawMsg.MessageBody, &objectNext)
        msg.MessageBody = objectNext
    case SYNC_PUSH_MESSAGE:
        var pushMessage PushMessage
        err = json.Unmarshal(rawMsg.MessageBody, &pushMessage)
        msg.MessageBody = pushMessage
    case SYNC_PUSH_DONE:
        var pushDoneMessage PushDone
        err = json.Unmarshal(rawMsg.MessageBody, &pushDoneMessage)
        msg.MessageBody = pushDoneMessage
    }
    
    return err
}

func (peer *Peer) close(closeCode int) {
    peer.csLock.Lock()
    defer peer.csLock.Unlock()

    if !peer.closed {
        peer.closeChan <- true
        peer.closed = true
    }
        
    if peer.connection != nil {
        err := peer.connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(closeCode, ""))
        
        if err != nil {
            return
        }
            
        select {
        case <-peer.doneChan:
        case <-time.After(time.Second):
        }
        
        peer.connection.Close()
    }
}

func (peer *Peer) isClosed() bool {
    return peer.closed
}

func (peer *Peer) toJSON(peerID string) *PeerJSON {
    var direction string
    var status string
    
    if peer.direction == INCOMING {
        direction = "incoming"
    } else {
        direction = "outgoing"
    }
    
    if peer.connection == nil {
        status = "down"
    } else {
        status = "up"
    }
    
    return &PeerJSON{
        Direction: direction,
        Status: status,
        ID: peerID,
    }
}

func (peer *Peer) useHistoryServer(tlsBaseConfig *tls.Config, historyServerName string, historyHost string, historyPort int, noValidate bool) {
    peer.historyHost = historyHost
    peer.historyPort = historyPort
   
    tlsConfig := *tlsBaseConfig
    tlsConfig.InsecureSkipVerify = noValidate
    tlsConfig.ServerName = historyServerName
    
    peer.httpHistoryClient = &http.Client{ Transport: &http.Transport{ TLSClientConfig: &tlsConfig } }
}

func (peer *Peer) getLatestEventSerial(hubID string) (uint64, error) {
    if peer.id != CLOUD_PEER_ID {
        return 0, errors.New("This peer is not the cloud peer")
    }
    
    resp, err := peer.httpHistoryClient.Get(fmt.Sprintf("https://%s:%d/abc/events/%s/latestSerial", peer.historyHost, peer.historyPort, hubID))
    
    if err != nil {
        return 0, err
    }
    
    defer resp.Body.Close()
    responseBody, err := ioutil.ReadAll(resp.Body)
        
    if err != nil {
        return 0, err
    }
    
    if resp.StatusCode != http.StatusOK {
        return 0, errors.New(fmt.Sprintf("Received error code from server: (%d) %s", resp.StatusCode, string(responseBody)))
    }
    
    latestSerial, err := strconv.ParseUint(string(responseBody), 10, 64)
    
    if err != nil {
        return 0, err
    }
    
    return latestSerial, nil
}

func (peer *Peer) getLatestAlertSerial(hubID string) (uint64, error) {
    if peer.id != CLOUD_PEER_ID {
        return 0, errors.New("This peer is not the cloud peer")
    }
    
    resp, err := peer.httpHistoryClient.Get(fmt.Sprintf("https://%s:%d/abc/alerts/%s/latestSerial", peer.historyHost, peer.historyPort, hubID))
    
    if err != nil {
        return 0, err
    }
    
    defer resp.Body.Close()
    responseBody, err := ioutil.ReadAll(resp.Body)
        
    if err != nil {
        return 0, err
    }
    
    if resp.StatusCode != http.StatusOK {
        return 0, errors.New(fmt.Sprintf("Received error code from server: (%d) %s", resp.StatusCode, string(responseBody)))
    }
    
    latestSerial, err := strconv.ParseUint(string(responseBody), 10, 64)
    
    if err != nil {
        return 0, err
    }
    
    return latestSerial, nil
}

func (peer *Peer) pushEvent(hubID string, event *Event) error {
    // try to forward event to the cloud if failed or error response then return
    eventJSON, _ := json.Marshal(event)
    request, err := http.NewRequest("PUT", fmt.Sprintf("https://%s:%d/abc/events/%s/%s/%s", peer.historyHost, peer.historyPort, hubID, event.SourceID, event.Type), bytes.NewReader(eventJSON))
    
    if err != nil {
        return err
    }
    
    request.Header.Add("Content-Type", "application/json")
    
    resp, err := peer.httpHistoryClient.Do(request)
    
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

func (peer *Peer) pushAlert(hubID string, event *Event) error {
    // try to forward event to the cloud if failed or error response then return
    eventJSON, _ := json.Marshal(event)
    request, err := http.NewRequest("PUT", fmt.Sprintf("https://%s:%d/abc/alerts/%s/%s/%s", peer.historyHost, peer.historyPort, hubID, event.SourceID, event.Type), bytes.NewReader(eventJSON))
    
    if err != nil {
        return err
    }
    
    request.Header.Add("Content-Type", "application/json")
    
    resp, err := peer.httpHistoryClient.Do(request)
    
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

func closeWSConnection(conn *websocket.Conn) {
    done := make(chan bool)
    
    go func() {
        defer close(done)
        
        for {
            _, _, err := conn.ReadMessage()
            
            if err != nil {
                return
            }
        }
    }()
            
    err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
        
    if err != nil {
        return
    }
    
    select {
    case <-done:
    case <-time.After(time.Second):
    }
    
    conn.Close()
}

type Hub struct {
    id string
    tlsConfig *tls.Config
    peerMapLock sync.Mutex
    peerMap map[string]*Peer
    peerMapByPartitionNumber map[uint64]map[string]*Peer
    peerMapBySiteID map[string]map[string]*Peer
    syncController *SyncController
    forwardEvents chan int
    forwardAlerts chan int
    historian *Historian
    alertsLog *Historian
    purgeOnForward bool
}

func NewHub(id string, syncController *SyncController, tlsConfig *tls.Config) *Hub {
    hub := &Hub{
        syncController: syncController,
        tlsConfig: tlsConfig,
        peerMap: make(map[string]*Peer),
        peerMapByPartitionNumber: make(map[uint64]map[string]*Peer),
        peerMapBySiteID: make(map[string]map[string]*Peer),
        id: id,
        forwardEvents: make(chan int, 1),
        forwardAlerts: make(chan int, 1),
    }
    
    return hub
}

func (hub *Hub) Accept(connection *websocket.Conn, partitionNumber uint64, relayID string, siteID string, noValidate bool) error {
    conn := connection.UnderlyingConn()
    
    if _, ok := conn.(*tls.Conn); ok || relayID != "" {
        var peerID string
        var err error

        if _, ok := conn.(*tls.Conn); ok {
            peerID, err = hub.ExtractPeerID(conn.(*tls.Conn))
        } else {
            peerID = relayID
        }
        
        if err != nil {
            if !noValidate {
                Log.Warningf("Unable to accept peer connection: %v", err)
                
                closeWSConnection(connection)
                
                return err
            }

            peerID = relayID
        }

        if noValidate && relayID != "" {
            peerID = relayID
        }

        if peerID == "" {
            Log.Warningf("Unable to accept peer connection")

            closeWSConnection(connection)

            return errors.New("Relay id not known")
        }

        go func() {
            peer := NewPeer(peerID, INCOMING)
            peer.partitionNumber = partitionNumber
            peer.siteID = siteID
            
            if !hub.register(peer) {
                Log.Warningf("Rejected peer connection from %s because that peer is already connected", peerID)
                
                closeWSConnection(connection)
                
                return
            }
            
            incoming, outgoing, err := peer.accept(connection)
            
            if err != nil {
                Log.Errorf("Unable to accept peer connection from %s: %v. Closing connection and unregistering peer", peerID, err)

                closeWSConnection(connection)

                hub.unregister(peer)
                
                return
            }
            
            Log.Infof("Accepted peer connection from %s", peerID)
            
            hub.syncController.addPeer(peer.id, outgoing)
                
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
            
            hub.syncController.removePeer(peer.id)
            hub.unregister(peer)
            
            Log.Infof("Disconnected from peer %s", peerID)
        }()
    } else {
        return errors.New("Cannot accept non-secure connections")
    }
        
    return nil
}

func (hub *Hub) ConnectCloud(serverName, host string, port int, historyServerName, historyHost string, historyPort int, noValidate bool) error {
    if noValidate {
        Log.Warningf("The cloud.noValidate option is set to true. The cloud server's certificate chain and identity will not be verified. !!! THIS OPTION SHOULD NOT BE SET TO TRUE IN PRODUCTION !!!")
    }
    
    dialer, err := hub.dialer(serverName, noValidate, true)
    
    if err != nil {
        return err
    }
    
    go func() {
        peer := NewPeer(CLOUD_PEER_ID, OUTGOING)
    
        // simply try to reserve a spot in the peer map
        if !hub.register(peer) {
            return
        }
    
        for {
            // connect will return an error once the peer is disconnected for good
            peer.useHistoryServer(hub.tlsConfig, historyServerName, historyHost, historyPort, noValidate)
            peer.identityHeader = hub.id
            incoming, outgoing, err := peer.connect(dialer, host, port)
            
            if err != nil {
                break
            }
            
            Log.Infof("Connected to devicedb cloud")
            
            hub.syncController.addPeer(peer.id, outgoing)
        
            // incoming is closed when the peer is disconnected from either end
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
        
            hub.syncController.removePeer(peer.id)
            
            if websocket.IsCloseError(peer.errors(), websocket.CloseNormalClosure) {
                Log.Infof("Disconnected from devicedb cloud")
            
                break
            }
            
            Log.Infof("Disconnected from devicedb cloud. Reconnecting...")
            <-time.After(time.Second)
        }
        
        hub.unregister(peer)
    }()
    
    return nil
}

func (hub *Hub) Connect(peerID, host string, port int) error {
    dialer, err := hub.dialer(peerID, false, false)
    
    if peerID == CLOUD_PEER_ID {
        Log.Warningf("Peer ID is not allowed to be %s since it is reserved for the cloud connection. This node will not connect to this peer", CLOUD_PEER_ID)
        
        return errors.New("Peer ID is not allowed to be " + CLOUD_PEER_ID)
    }
    
    if err != nil {
        return err
    }    
    
    go func() {
        peer := NewPeer(peerID, OUTGOING)
    
        // simply try to reserve a spot in the peer map
        if !hub.register(peer) {
            return
        }
    
        for {
            // connect will return an error once the peer is disconnected for good
            incoming, outgoing, err := peer.connect(dialer, host, port)
            
            if err != nil {
                break
            }
            
            Log.Infof("Connected to peer %s", peer.id)
            
            hub.syncController.addPeer(peer.id, outgoing)
        
            // incoming is closed when the peer is disconnected from either end
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
        
            hub.syncController.removePeer(peer.id)
            
            if websocket.IsCloseError(peer.errors(), websocket.CloseNormalClosure) {
                Log.Infof("Disconnected from peer %s", peer.id)
            
                break
            }
            
            Log.Infof("Disconnected from peer %s. Reconnecting...", peer.id)
        }
        
        hub.unregister(peer)
    }()
    
    return nil
}

func (hub *Hub) Disconnect(peerID string) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()
    
    peer, ok := hub.peerMap[peerID]
    
    if ok {
        peer.close(websocket.CloseNormalClosure)
    }
}

func (hub *Hub) PeerStatus(peerID string) (connected bool, pingTime time.Duration) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    peer, ok := hub.peerMap[peerID]
    
    if ok {
        return true, peer.getRoundTripTime()
    }

    return false, 0
}

func (hub *Hub) ReconnectPeer(peerID string) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()
    
    peer, ok := hub.peerMap[peerID]
    
    if ok {
        peer.close(websocket.CloseTryAgainLater)
    }
}

func (hub *Hub) ReconnectPeerByPartition(partitionNumber uint64) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    if peers, ok := hub.peerMapByPartitionNumber[partitionNumber]; ok {
        for _, peer := range peers {
            peer.close(websocket.CloseTryAgainLater)
        }
    }
}

func (hub *Hub) ReconnectPeerBySite(siteID string) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    if peers, ok := hub.peerMapBySiteID[siteID]; ok {
        for _, peer := range peers {
            peer.close(websocket.CloseTryAgainLater)
        }
    }
}

func (hub *Hub) dialer(peerID string, noValidate bool, useDefaultRootCAs bool) (*websocket.Dialer, error) {
    if hub.tlsConfig == nil {
        return nil, errors.New("No tls config provided")
    }
    
    tlsConfig := *hub.tlsConfig
    
    if useDefaultRootCAs {
        tlsConfig.RootCAs = nil
    }
    
    tlsConfig.InsecureSkipVerify = noValidate
    tlsConfig.ServerName = peerID
    
    dialer := &websocket.Dialer{
        TLSClientConfig: &tlsConfig,
    }
    
    return dialer, nil
}

func (hub *Hub) register(peer *Peer) bool {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()
    
    if _, ok := hub.peerMap[peer.id]; ok {
        return false
    }
    
    Log.Debugf("Register peer %s", peer.id)
    hub.peerMap[peer.id] = peer
    
    if _, ok := hub.peerMapByPartitionNumber[peer.partitionNumber]; !ok {
        hub.peerMapByPartitionNumber[peer.partitionNumber] = make(map[string]*Peer)
    }

    if _, ok := hub.peerMapBySiteID[peer.siteID]; !ok {
        hub.peerMapBySiteID[peer.siteID] = make(map[string]*Peer)
    }

    hub.peerMapByPartitionNumber[peer.partitionNumber][peer.id] = peer
    hub.peerMapBySiteID[peer.siteID][peer.id] = peer
    
    return true
}

func (hub *Hub) unregister(peer *Peer) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    if _, ok := hub.peerMap[peer.id]; ok {
        Log.Debugf("Unregister peer %s", peer.id)
    }
    
    delete(hub.peerMap, peer.id)
    delete(hub.peerMapByPartitionNumber[peer.partitionNumber], peer.id)
    delete(hub.peerMapBySiteID[peer.siteID], peer.id)

    if len(hub.peerMapByPartitionNumber[peer.partitionNumber]) == 0 {
        delete(hub.peerMapByPartitionNumber, peer.partitionNumber)
    }

    if len(hub.peerMapBySiteID[peer.siteID]) == 0 {
        delete(hub.peerMapBySiteID, peer.siteID)
    }
}

func (hub *Hub) Peers() []*PeerJSON {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()
    peers := make([]*PeerJSON, 0, len(hub.peerMap))
    
    for peerID, ps := range hub.peerMap {
        peers = append(peers, ps.toJSON(peerID))
    }
    
    return peers
}

func (hub *Hub) ExtractPeerID(conn *tls.Conn) (string, error) {
    // VerifyClientCertIfGiven
    verifiedChains := conn.ConnectionState().VerifiedChains
    
    if len(verifiedChains) != 1 {
        return "", errors.New("Invalid client certificate")
    }
    
    peerID := verifiedChains[0][0].Subject.CommonName
    
    return peerID, nil
}

func (hub *Hub) SyncController() *SyncController {
    return hub.syncController
}

func (hub *Hub) ForwardEvents() {
    select {
    case hub.forwardEvents <- 1:
    default:
    }
}

func (hub *Hub) StartForwardingEvents() {
    go func() {
        for {
            <-hub.forwardEvents
            Log.Info("Begin event forwarding to the cloud")
            
            var cloudPeer *Peer
            
            hub.peerMapLock.Lock()
            cloudPeer, ok := hub.peerMap[CLOUD_PEER_ID]
            hub.peerMapLock.Unlock()
            
            if !ok {
                Log.Info("No cloud present. Nothing to forward to")
                
                continue
            }
            
            cloudSerial, err := cloudPeer.getLatestEventSerial(hub.id)
            
            if err != nil {
                Log.Warningf("Unable to get the latest event serial from the cloud. Event forwarding will resume later: %v", err)
                
                continue
            }
        
            if cloudSerial > hub.historian.LogSerial() - 1 {
                Log.Warningf("The last event that the cloud received from this peer had a serial number greater than any event this node has stored in its history. This may indicate that the data store at this node was wiped since last connecting to the cloud. Skipping event serial number to %d", cloudSerial)
                
                err := hub.historian.SetLogSerial(cloudSerial)
                
                if err != nil {
                    Log.Errorf("Unable to skip event log serial number ahead from %d to %d: %v. No new events will be forwarded to the cloud.", hub.historian.LogSerial() - 1, cloudSerial, err)
                    
                    return
                }
            }
            
            for cloudSerial < hub.historian.LogSerial() - 1 {
                minSerial := cloudSerial + 1
                eventIterator, err := hub.historian.Query(&HistoryQuery{ MinSerial: &minSerial, Limit: 1 })
                
                if err != nil {
                    Log.Errorf("Unable to query event history: %v. No more events will be forwarded to the cloud", err)
                    
                    return
                }
                
                if eventIterator.Next() {
                    err := cloudPeer.pushEvent(hub.id, eventIterator.Event())
                    
                    if err != nil {
                        Log.Warningf("Unable to push event %d to the cloud: %v. Event forwarding process will resume later.", eventIterator.Event().Serial, err)
                        
                        break
                    }
                    
                    if hub.purgeOnForward {
                        maxSerial := eventIterator.Event().Serial + 1
                        err = hub.historian.Purge(&HistoryQuery{ MaxSerial: &maxSerial })
                        
                        if err != nil {
                            Log.Warningf("Unable to purge events after push: %v")
                        }
                    }
                }
                
                if eventIterator.Error() != nil {
                    Log.Errorf("Unable to query event history. Event iterator error: %v. No more events will be forwarded to the cloud.", eventIterator.Error())
                    
                    return
                }
                
                cloudSerial, err = cloudPeer.getLatestEventSerial(hub.id)
                
                if err != nil {
                    Log.Warningf("Unable to get the latest event serial from the cloud. Event forwarding will resume later: %v", err)
                    
                    break
                }
            }
            
            Log.Info("History forwarding complete. Sleeping...")
        }
    }()
}

func (hub *Hub) ForwardAlerts() {
    select {
    case hub.forwardAlerts <- 1:
    default:
    }
}

func (hub *Hub) StartForwardingAlerts() {
    go func() {
        for {
            <-hub.forwardAlerts
            Log.Info("Begin alert forwarding to the cloud")
            
            var cloudPeer *Peer
            
            hub.peerMapLock.Lock()
            cloudPeer, ok := hub.peerMap[CLOUD_PEER_ID]
            hub.peerMapLock.Unlock()
            
            if !ok {
                Log.Info("No cloud present. Nothing to forward to")
                
                continue
            }
            
            cloudSerial, err := cloudPeer.getLatestAlertSerial(hub.id)
            
            if err != nil {
                Log.Warningf("Unable to get the latest alert serial from the cloud. Event forwarding will resume later: %v", err)
                
                continue
            }
        
            if cloudSerial > hub.alertsLog.LogSerial() - 1 {
                Log.Warningf("The last event that the cloud received from this peer had a serial number greater than any event this node has stored in its history. This may indicate that the data store at this node was wiped since last connecting to the cloud. Skipping event serial number to %d", cloudSerial)
                
                err := hub.alertsLog.SetLogSerial(cloudSerial)
                
                if err != nil {
                    Log.Errorf("Unable to skip alert log serial number ahead from %d to %d: %v. No new alerts will be forwarded to the cloud.", hub.alertsLog.LogSerial() - 1, cloudSerial, err)
                    
                    return
                }
            }
            
            for cloudSerial < hub.alertsLog.LogSerial() - 1 {
                minSerial := cloudSerial + 1
                alertIterator, err := hub.alertsLog.Query(&HistoryQuery{ MinSerial: &minSerial, Limit: 1 })
                
                if err != nil {
                    Log.Errorf("Unable to query alert history: %v. No more alerts will be forwarded to the cloud", err)
                    
                    return
                }
                
                if alertIterator.Next() {
                    err := cloudPeer.pushAlert(hub.id, alertIterator.Event())
                    
                    if err != nil {
                        Log.Warningf("Unable to push alert %d to the cloud: %v. Alert forwarding process will resume later.", alertIterator.Event().Serial, err)
                        
                        break
                    }
                    
                    if hub.purgeOnForward {
                        maxSerial := alertIterator.Event().Serial + 1
                        err = hub.alertsLog.Purge(&HistoryQuery{ MaxSerial: &maxSerial })
                        
                        if err != nil {
                            Log.Warningf("Unable to purge alerts after push: %v")
                        }
                    }
                }
                
                if alertIterator.Error() != nil {
                    Log.Errorf("Unable to query alert history. Alert iterator error: %v. No more alerts will be forwarded to the cloud.", alertIterator.Error())
                    
                    return
                }
                
                cloudSerial, err = cloudPeer.getLatestAlertSerial(hub.id)
                
                if err != nil {
                    Log.Warningf("Unable to get the latest alert serial from the cloud. Alert forwarding will resume later: %v", err)
                    
                    break
                }
            }
            
            Log.Info("Alert forwarding complete. Sleeping...")
        }
    }()
}

func (hub *Hub) BroadcastUpdate(siteID string, bucket string, update map[string]*SiblingSet, n uint64) {
    // broadcast the specified update to at most n peers, or all peers if n is non-positive
    var count uint64 = 0

    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    peers := hub.peerMapBySiteID[siteID]

    for peerID, _ := range peers {
        if !hub.syncController.bucketProxyFactory.OutgoingBuckets(peerID)[bucket] {
            continue
        }

        if n != 0 && count == n {
            break
        }

        hub.syncController.BroadcastUpdate(peerID, bucket, update, n)
        count += 1
    }
}

type SyncSession struct {
    receiver chan *SyncMessageWrapper
    sender chan *SyncMessageWrapper
    sessionState interface{ }
    waitGroup *sync.WaitGroup
    peerID string
    sessionID uint
}

type SyncController struct {
    bucketProxyFactory ddbSync.BucketProxyFactory
    incoming chan *SyncMessageWrapper
    peers map[string]chan *SyncMessageWrapper
    waitGroups map[string]*sync.WaitGroup
    initiatorSessionsMap map[string]map[uint]*SyncSession
    responderSessionsMap map[string]map[uint]*SyncSession
    initiatorSessions chan *SyncSession
    responderSessions chan *SyncSession
    maxSyncSessions uint
    nextSessionID uint
    mapMutex sync.RWMutex
    syncScheduler ddbSync.SyncScheduler
    explorationPathLimit uint32
}

func NewSyncController(maxSyncSessions uint, bucketProxyFactory ddbSync.BucketProxyFactory, syncScheduler ddbSync.SyncScheduler, explorationPathLimit uint32) *SyncController {
    syncController := &SyncController{
        bucketProxyFactory: bucketProxyFactory,
        incoming: make(chan *SyncMessageWrapper),
        peers: make(map[string]chan *SyncMessageWrapper),
        waitGroups: make(map[string]*sync.WaitGroup),
        initiatorSessionsMap: make(map[string]map[uint]*SyncSession),
        responderSessionsMap: make(map[string]map[uint]*SyncSession),
        initiatorSessions: make(chan *SyncSession),
        responderSessions: make(chan *SyncSession),
        maxSyncSessions: maxSyncSessions,
        nextSessionID: 1,
        syncScheduler: syncScheduler,
        explorationPathLimit: explorationPathLimit,
    }
    
    go func() {
        // multiplex incoming messages accross sync sessions
        for msg := range syncController.incoming {
            syncController.receiveMessage(msg)
        }
    }()
    
    return syncController
}

func (s *SyncController) addPeer(peerID string, w chan *SyncMessageWrapper) error {
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.peers[peerID]; ok {
        return errors.New("Peer already registered")
    }
    
    s.peers[peerID] = w
    s.waitGroups[peerID] = &sync.WaitGroup{ }
    s.initiatorSessionsMap[peerID] = make(map[uint]*SyncSession)
    s.responderSessionsMap[peerID] = make(map[uint]*SyncSession)

    var buckets []string = make([]string, 0, len(s.bucketProxyFactory.IncomingBuckets(peerID)))

    for bucket, _ := range s.bucketProxyFactory.IncomingBuckets(peerID) {
        buckets = append(buckets, bucket)
    }

    s.syncScheduler.AddPeer(peerID, buckets)
    s.syncScheduler.Schedule(peerID)
    
    return nil
}

func (s *SyncController) removePeer(peerID string) {
    s.mapMutex.Lock()
    
    if _, ok := s.peers[peerID]; !ok {
        return
    }
    
    for _, syncSession := range s.initiatorSessionsMap[peerID] {
        close(syncSession.receiver)
    }
    
    for _, syncSession := range s.responderSessionsMap[peerID] {
        close(syncSession.receiver)
    }

    delete(s.initiatorSessionsMap, peerID)
    delete(s.responderSessionsMap, peerID)

    s.syncScheduler.RemovePeer(peerID)

    wg := s.waitGroups[peerID]
    s.mapMutex.Unlock()
    wg.Wait()

    s.mapMutex.Lock()
    close(s.peers[peerID])
    delete(s.peers, peerID)
    delete(s.waitGroups, peerID)
    s.mapMutex.Unlock()
}

func (s *SyncController) addResponderSession(peerID string, sessionID uint, bucketName string) bool {
    if !s.bucketProxyFactory.OutgoingBuckets(peerID)[bucketName] {
        Log.Errorf("Unable to add responder session %d for peer %s because bucket %s does not allow outgoing messages to this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucketProxy, err := s.bucketProxyFactory.CreateBucketProxy(peerID, bucketName)

    if err != nil {
        Log.Errorf("Unable to add responder session %d for peer %s because a bucket proxy could not be created for bucket %s: %v", sessionID, peerID, bucketName, err)

        return false
    }
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.responderSessionsMap[peerID]; !ok {
        Log.Errorf("Unable to add responder session %d for peer %s because this peer is not registered with the sync controller", sessionID, peerID)
        
        return false
    }
    
    if _, ok := s.responderSessionsMap[peerID][sessionID]; ok {
        Log.Errorf("Unable to add responder session %d for peer %s because a responder session with this id for this peer already exists", sessionID, peerID)
        
        return false
    }
    
    newResponderSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper, 1),
        sender: s.peers[peerID],
        sessionState: NewResponderSyncSession(bucketProxy),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    s.responderSessionsMap[peerID][sessionID] = newResponderSession
    newResponderSession.waitGroup.Add(1)
    
    select {
    case s.responderSessions <- newResponderSession:
        Log.Infof("Added responder session %d for peer %s and bucket %s", sessionID, peerID, bucketName)
        
        return true
    default:
        Log.Warningf("Unable to add responder session %d for peer %s and bucket %s because there are already %d responder sync sessions", sessionID, peerID, bucketName, s.maxSyncSessions)
        
        delete(s.responderSessionsMap[peerID], sessionID)
        newResponderSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) addInitiatorSession(peerID string, sessionID uint, bucketName string) bool {
    if !s.bucketProxyFactory.IncomingBuckets(peerID)[bucketName] {
        Log.Errorf("Unable to add initiator session %d for peer %s because bucket %s does not allow incoming messages from this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucketProxy, err := s.bucketProxyFactory.CreateBucketProxy(peerID, bucketName)
    
    if err != nil {
        Log.Errorf("Unable to add initiator session %d for peer %s because a bucket proxy could not be created for bucket %s: %v", sessionID, peerID, bucketName, err)

        return false
    }

    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.initiatorSessionsMap[peerID]; !ok {
        Log.Errorf("Unable to add initiator session %d for peer %s because this peer is not registered with the sync controller", sessionID, peerID)
        
        return false
    }
    
    newInitiatorSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper, 1),
        sender: s.peers[peerID],
        sessionState: NewInitiatorSyncSession(sessionID, bucketProxy, s.explorationPathLimit, s.bucketProxyFactory.OutgoingBuckets(peerID)[bucketName]),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    // check map to see if it has this one
    if _, ok := s.initiatorSessionsMap[peerID][sessionID]; ok {
        Log.Errorf("Unable to add initiator session %d for peer %s because an initiator session with this id for this peer already exists", sessionID, peerID)
        
        return false
    }
    
    s.initiatorSessionsMap[peerID][sessionID] = newInitiatorSession
    newInitiatorSession.waitGroup.Add(1)
    
    select {
    case s.initiatorSessions <- newInitiatorSession:
        Log.Infof("Added initiator session %d for peer %s and bucket %s", sessionID, peerID, bucketName)
        
        s.syncScheduler.Advance()
        s.initiatorSessionsMap[peerID][sessionID].receiver <- nil
        
        return true
    default:
        Log.Warningf("Unable to add initiator session %d for peer %s and bucket %s because there are already %d initiator sync sessions", sessionID, peerID, bucketName, s.maxSyncSessions)
        
        delete(s.initiatorSessionsMap[peerID], sessionID)
        newInitiatorSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) removeResponderSession(responderSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.responderSessionsMap[responderSession.peerID]; ok {
        delete(s.responderSessionsMap[responderSession.peerID], responderSession.sessionID)
    }
    
    s.mapMutex.Unlock()
    responderSession.waitGroup.Done()
    
    Log.Infof("Removed responder session %d for peer %s", responderSession.sessionID, responderSession.peerID)
}

func (s *SyncController) removeInitiatorSession(initiatorSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.initiatorSessionsMap[initiatorSession.peerID]; ok {
        s.syncScheduler.Schedule(initiatorSession.peerID)
        delete(s.initiatorSessionsMap[initiatorSession.peerID], initiatorSession.sessionID)
    }
    
    s.mapMutex.Unlock()
    initiatorSession.waitGroup.Done()
    
    Log.Infof("Removed initiator session %d for peer %s", initiatorSession.sessionID, initiatorSession.peerID)
}

func (s *SyncController) sendAbort(peerID string, sessionID uint, direction uint) {
    s.peers[peerID] <- &SyncMessageWrapper{
        SessionID: sessionID,
        MessageType: SYNC_ABORT,
        MessageBody: &Abort{ },
        Direction: direction,
    }
}

func (s *SyncController) receiveMessage(msg *SyncMessageWrapper) {
    nodeID := msg.nodeID
    sessionID := msg.SessionID    
    
    if msg.Direction == REQUEST {
        if msg.MessageType == SYNC_START {
            if !s.addResponderSession(nodeID, sessionID, msg.MessageBody.(Start).Bucket) {
                s.sendAbort(nodeID, sessionID, RESPONSE)
            }
        }
        
        s.mapMutex.RLock()
        defer s.mapMutex.RUnlock()
        
        if _, ok := s.responderSessionsMap[nodeID]; !ok {
            // s.sendAbort(nodeID, sessionID, RESPONSE)
            
            return
        }
        
        if _, ok := s.responderSessionsMap[nodeID][sessionID]; !ok {
            // s.sendAbort(nodeID, sessionID, RESPONSE)
            
            return
        }
        
        // This select statement means that the function does not block if
        // the read loop in runInitiatorSession is not running if the session
        // is being removed currently. This works due to the synchronous nature
        // of the protocol.
        select {
        case s.responderSessionsMap[nodeID][sessionID].receiver <- msg:
        default:
        }
    } else if msg.Direction == RESPONSE { // RESPONSE
        s.mapMutex.RLock()
        defer s.mapMutex.RUnlock()
        
        if _, ok := s.initiatorSessionsMap[nodeID]; !ok {
            // s.sendAbort(nodeID, sessionID, REQUEST)
            
            return
        }
        
        if _, ok := s.initiatorSessionsMap[nodeID][sessionID]; !ok {
            // s.sendAbort(nodeID, sessionID, REQUEST)
            
            return
        }
        
        select {
        case s.initiatorSessionsMap[nodeID][sessionID].receiver <- msg:
        default:
        }
    } else if msg.MessageType == SYNC_PUSH_MESSAGE {
        pushMessage := msg.MessageBody.(PushMessage)
        
        if !s.bucketProxyFactory.IncomingBuckets(nodeID)[pushMessage.Bucket] {
            Log.Errorf("Ignoring push message from %s because this node does not accept incoming pushes from bucket %s from that node", nodeID, pushMessage.Bucket)
            
            return
        }

        key := pushMessage.Key
        value := pushMessage.Value
        bucketProxy, err := s.bucketProxyFactory.CreateBucketProxy(nodeID, pushMessage.Bucket)

        if err != nil {
            Log.Errorf("Ignoring push message from %s for bucket %s because an error occurred while creating a bucket proxy: %v", nodeID, pushMessage.Bucket, err)

            return
        }

        err = bucketProxy.Merge(map[string]*SiblingSet{ key: value })
        
        if err != nil {
            Log.Errorf("Unable to merge object from peer %s into key %s in bucket %s: %v", nodeID, key, pushMessage.Bucket, err)
        } else {
            Log.Infof("Merged object from peer %s into key %s in bucket %s", nodeID, key, pushMessage.Bucket)
        }
    }
}

func (s *SyncController) runInitiatorSession() {
    for initiatorSession := range s.initiatorSessions {
        state := initiatorSession.sessionState.(*InitiatorSyncSession)
        
        for {
            var receivedMessage *SyncMessageWrapper
            
            select {
            case receivedMessage = <-initiatorSession.receiver:
            case <-time.After(time.Second * SYNC_SESSION_WAIT_TIMEOUT_SECONDS):
                Log.Warningf("[%s-%d] timeout", initiatorSession.peerID, initiatorSession.sessionID)
            }
            
            initialState := state.State()
            
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
            
            m.Direction = REQUEST
    
            if receivedMessage == nil {
                Log.Debugf("[%s-%d] nil : (%s -> %s) : %s", initiatorSession.peerID, initiatorSession.sessionID, StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            } else {
                Log.Debugf("[%s-%d] %s : (%s -> %s) : %s", initiatorSession.peerID, initiatorSession.sessionID, MessageTypeName(receivedMessage.MessageType), StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            }
            
            if m != nil {
                initiatorSession.sender <- m
            }
            
            if state.State() == END {
                break
            }
        }
        
        s.removeInitiatorSession(initiatorSession)
    }
}

func (s *SyncController) runResponderSession() {
    for responderSession := range s.responderSessions {
        state := responderSession.sessionState.(*ResponderSyncSession)
        
        for {
            var receivedMessage *SyncMessageWrapper
            
            select {
            case receivedMessage = <-responderSession.receiver:
            case <-time.After(time.Second * SYNC_SESSION_WAIT_TIMEOUT_SECONDS):
                Log.Warningf("[%s-%d] timeout", responderSession.peerID, responderSession.sessionID)
            }
            
            initialState := state.State()
            
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
        
            m.Direction = RESPONSE
            
            if receivedMessage == nil {
                Log.Debugf("[%s-%d] nil : (%s -> %s) : %s", responderSession.peerID, responderSession.sessionID, StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            } else {
                Log.Debugf("[%s-%d] %s : (%s -> %s) : %s", responderSession.peerID, responderSession.sessionID, MessageTypeName(receivedMessage.MessageType), StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            }
            
            if m != nil {
                responderSession.sender <- m
            }
            
            if state.State() == END {
                break
            }
        }
        
        s.removeResponderSession(responderSession)
    }
}

func (s *SyncController) StartInitiatorSessions() {
    for i := 0; i < int(s.maxSyncSessions); i += 1 {
        go s.runInitiatorSession()
    }
    
    go func() {
        for {
            peerID, bucketName := s.syncScheduler.Next()
            
            s.mapMutex.RLock()

            if peerID == "" {
                s.mapMutex.RUnlock()

                continue
            }

            s.mapMutex.RUnlock()
            
            if s.addInitiatorSession(peerID, s.nextSessionID, bucketName) {
                s.nextSessionID += 1
            } else {
            }
        }
    }()
}

func (s *SyncController) StartResponderSessions() {
    for i := 0; i < int(s.maxSyncSessions); i += 1 {
        go s.runResponderSession()
    }    
}

func (s *SyncController) Start() {
    s.StartInitiatorSessions()
    s.StartResponderSessions()
}

func (s *SyncController) BroadcastUpdate(peerID string, bucket string, update map[string]*SiblingSet, n uint64) {    
    s.mapMutex.RLock()
    defer s.mapMutex.RUnlock()
   
    for key, value := range update {
        msg := &SyncMessageWrapper{
            SessionID: 0,
            MessageType: SYNC_PUSH_MESSAGE,
            MessageBody: PushMessage{
                Key: key,
                Value: value,
                Bucket: bucket,
            },
            Direction: PUSH,
        }
       
        w := s.peers[peerID]

        Log.Debugf("Push object at key %s in bucket %s to peer %s", key, bucket, peerID)
        w <- msg
    }
}
