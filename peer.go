package devicedb

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
)

const (
    INCOMING = iota
    OUTGOING = iota
)

const SYNC_SESSION_WAIT_TIMEOUT_SECONDS = 5
const RECONNECT_WAIT_MAX_SECONDS = 32
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
    result error
    host string
    port int
    httpClient *http.Client
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
    
    for {
        peer.connection = nil

        conn, _, err := dialer.Dial("wss://" + host + ":" + strconv.Itoa(port) + "/sync", nil)
                
        if err != nil {
            log.Warningf("Unable to connect to peer %s at %s on port %d: %v. Reconnecting in %ds...", peer.id, host, port, err, reconnectWaitSeconds)
            
            select {
            case <-time.After(time.Second * time.Duration(reconnectWaitSeconds)):
            case <-peer.closeChan:
                log.Debugf("Cancelled connection retry sequence for %s", peer.id)
                
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
            
            log.Debugf("Cancelled connection retry sequence for %s", peer.id)
            
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
        for msg := range outgoing {
            // this lock ensures mutual exclusion with close message sending in peer.close()
            peer.csLock.Lock()
            err := connection.WriteJSON(msg)
            peer.csLock.Unlock()
                
            if err != nil {
                log.Errorf("Error writing to websocket for peer %s: %v", peer.id, err)
                //return
            }
        }
    }()
    
    // incoming, outgoing, err
    go func() {
        defer close(peer.doneChan)
        
        for {
            var nextRawMessage rawSyncMessageWrapper
            var nextMessage SyncMessageWrapper
            
            err := connection.ReadJSON(&nextRawMessage)
            
            if err != nil {
                if err.Error() == "websocket: close 1000 (normal)" {
                    log.Infof("Received a normal websocket close message from peer %s", peer.id)
                } else {
                    log.Errorf("Peer %s sent a misformatted message. Unable to parse: %v", peer.id, err)
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

func (peer *Peer) close() {
    peer.csLock.Lock()
    defer peer.csLock.Unlock()
    
    if !peer.closed {
        peer.closeChan <- true
        peer.closed = true
    }
        
    if peer.connection != nil {
        err := peer.connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
        
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

func (peer *Peer) getLatestEventSerial(hubID string) (uint64, error) {
    if peer.id != CLOUD_PEER_ID {
        return 0, errors.New("This peer is not the cloud peer")
    }
    
    resp, err := peer.httpClient.Get(fmt.Sprintf("https://%s:%d/abc/events/%s/latestSerial", peer.host, peer.port, hubID))
    
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
    request, err := http.NewRequest("PUT", fmt.Sprintf("https://%s:%d/abc/events/%s/%s/%s", peer.host, peer.port, hubID, event.SourceID, event.Type), bytes.NewReader(eventJSON))
    
    if err != nil {
        return err
    }
    
    request.Header.Add("Content-Type", "application/json")
    
    resp, err := peer.httpClient.Do(request)
    
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
    syncController *SyncController
    forward chan int
    historian *Historian
    purgeOnForward bool
}

func NewHub(id string, syncController *SyncController, tlsConfig *tls.Config) *Hub {
    hub := &Hub{
        syncController: syncController,
        tlsConfig: tlsConfig,
        peerMap: make(map[string]*Peer),
        id: id,
        forward: make(chan int, 1),
    }
    
    return hub
}

func (hub *Hub) Accept(connection *websocket.Conn) error {
    conn := connection.UnderlyingConn()
    
    if _, ok := conn.(*tls.Conn); ok {
        peerID, err := hub.extractPeerID(conn.(*tls.Conn))
        
        if err != nil {
            log.Warningf("Unable to accept peer connection: %v", err)
            
            closeWSConnection(connection)
            
            return err
        }

        go func() {
            peer := NewPeer(peerID, INCOMING)
            
            if !hub.register(peer) {
                log.Warningf("Rejected peer connection from %s because that peer is already connected", peerID)
                
                closeWSConnection(connection)
                
                return
            }
            
            incoming, outgoing, err := peer.accept(connection)
            
            if err != nil {
                closeWSConnection(connection)
                
                return
            }
            
            log.Infof("Accepted peer connection from %s", peerID)
            
            hub.syncController.addPeer(peer.id, outgoing)
                
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
            
            hub.syncController.removePeer(peer.id)
            hub.unregister(peer)
            
            log.Infof("Disconnected from peer %s", peerID)
        }()
    } else {
        return errors.New("Cannot accept non-secure connections")
    }
        
    return nil
}

func (hub *Hub) ConnectCloud(serverName, host string, port int, noValidate bool) error {
    if noValidate {
        log.Warningf("The cloud.noValidate option is set to true. The cloud server's certificate chain and identity will not be verified. !!! THIS OPTION SHOULD NOT BE SET TO TRUE IN PRODUCTION !!!")
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
            incoming, outgoing, err := peer.connect(dialer, host, port)
            
            if err != nil {
                break
            }
            
            log.Infof("Connected to devicedb cloud")
            
            hub.syncController.addPeer(peer.id, outgoing)
        
            // incoming is closed when the peer is disconnected from either end
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
        
            hub.syncController.removePeer(peer.id)
            
            if websocket.IsCloseError(peer.errors(), websocket.CloseNormalClosure) {
                log.Infof("Disconnected from devicedb cloud")
            
                break
            }
            
            log.Infof("Disconnected from devicedb cloud. Reconnecting...")
        }
        
        hub.unregister(peer)
    }()
    
    return nil
}

func (hub *Hub) Connect(peerID, host string, port int) error {
    dialer, err := hub.dialer(peerID, false, false)
    
    if peerID == CLOUD_PEER_ID {
        log.Warningf("Peer ID is not allowed to be %s since it is reserved for the cloud connection. This node will not connect to this peer", CLOUD_PEER_ID)
        
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
            
            log.Infof("Connected to peer %s", peer.id)
            
            hub.syncController.addPeer(peer.id, outgoing)
        
            // incoming is closed when the peer is disconnected from either end
            for msg := range incoming {
                hub.syncController.incoming <- msg
            }
        
            hub.syncController.removePeer(peer.id)
            
            if websocket.IsCloseError(peer.errors(), websocket.CloseNormalClosure) {
                log.Infof("Disconnected from peer %s", peer.id)
            
                break
            }
            
            log.Infof("Disconnected from peer %s. Reconnecting...", peer.id)
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
        peer.close()
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
    
    log.Debugf("Register peer %s", peer.id)
    hub.peerMap[peer.id] = peer
    
    return true
}

func (hub *Hub) unregister(peer *Peer) {
    hub.peerMapLock.Lock()
    defer hub.peerMapLock.Unlock()

    if _, ok := hub.peerMap[peer.id]; ok {
        log.Debugf("Unregister peer %s", peer.id)
    }
    
    delete(hub.peerMap, peer.id)
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

func (hub *Hub) extractPeerID(conn *tls.Conn) (string, error) {
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
    case hub.forward <- 1:
    default:
    }
}

func (hub *Hub) StartForwardingEvents() {
    go func() {
        for {
            <-hub.forward
            log.Info("Begin event forwarding to the cloud")
            
            var cloudPeer *Peer
            
            hub.peerMapLock.Lock()
            cloudPeer, ok := hub.peerMap[CLOUD_PEER_ID]
            hub.peerMapLock.Unlock()
            
            if !ok {
                log.Info("No cloud present. Nothing to forward to")
                
                continue
            }
            
            cloudSerial, err := cloudPeer.getLatestEventSerial(hub.id)
            
            if err != nil {
                log.Warningf("Unable to get the latest event serial from the cloud. Event forwarding will resume later: %v", err)
                
                continue
            }
        
            if cloudSerial > hub.historian.LogSerial() - 1 {
                log.Warningf("The last event that the cloud received from this peer had a serial number greater than any event this node has stored in its history. This may indicate that the data store at this node was wiped since last connecting to the cloud. Skipping event serial number to %d", cloudSerial)
                
                err := hub.historian.SetLogSerial(cloudSerial)
                
                if err != nil {
                    log.Errorf("Unable to skip event log serial number ahead from %d to %d: %v. No new events will be forwarded to the cloud.", hub.historian.LogSerial() - 1, cloudSerial, err)
                    
                    return
                }
            }
            
            for cloudSerial < hub.historian.LogSerial() - 1 {
                minSerial := cloudSerial + 1
                eventIterator, err := hub.historian.Query(&HistoryQuery{ MinSerial: &minSerial, Limit: 1 })
                
                if err != nil {
                    log.Errorf("Unable to query event history: %v. No more events will be forwarded to the cloud", err)
                    
                    return
                }
                
                if eventIterator.Next() {
                    err := cloudPeer.pushEvent(hub.id, eventIterator.Event())
                    
                    if err != nil {
                        log.Warningf("Unable to push event %d to the cloud: %v. Event forwarding process will resume later.", eventIterator.Event().Serial, err)
                        
                        break
                    }
                    
                    if hub.purgeOnForward {
                        maxSerial := eventIterator.Event().Serial + 1
                        err = hub.historian.Purge(&HistoryQuery{ MaxSerial: &maxSerial })
                        
                        if err != nil {
                            log.Warningf("Unable to purge events after push: %v")
                        }
                    }
                }
                
                if eventIterator.Error() != nil {
                    log.Errorf("Unable to query event history. Event iterator error: %v. No more events will be forwarded to the cloud.", eventIterator.Error())
                    
                    return
                }
                
                cloudSerial, err = cloudPeer.getLatestEventSerial(hub.id)
                
                if err != nil {
                    log.Warningf("Unable to get the latest event serial from the cloud. Event forwarding will resume later: %v", err)
                    
                    break
                }
            }
            
            log.Info("History forwarding complete. Sleeping...")
        }
    }()
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
    buckets *BucketList
    incoming chan *SyncMessageWrapper
    peers map[string]chan *SyncMessageWrapper
    waitGroups map[string]*sync.WaitGroup
    initiatorSessionsMap map[string]map[uint]*SyncSession
    responderSessionsMap map[string]map[uint]*SyncSession
    initiatorSessions chan *SyncSession
    responderSessions chan *SyncSession
    syncBucketQueue [][]string
    maxSyncSessions uint
    nextSessionID uint
    mapMutex sync.RWMutex
    syncSessionPeriod uint64
    explorationPathLimit uint32
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList, syncSessionPeriod uint64, explorationPathLimit uint32) *SyncController {
    syncController := &SyncController{
        buckets: bucketList,
        incoming: make(chan *SyncMessageWrapper),
        peers: make(map[string]chan *SyncMessageWrapper),
        waitGroups: make(map[string]*sync.WaitGroup),
        initiatorSessionsMap: make(map[string]map[uint]*SyncSession),
        responderSessionsMap: make(map[string]map[uint]*SyncSession),
        initiatorSessions: make(chan *SyncSession),
        responderSessions: make(chan *SyncSession),
        syncBucketQueue: make([][]string, 0),
        maxSyncSessions: maxSyncSessions,
        nextSessionID: 1,
        syncSessionPeriod: syncSessionPeriod,
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

func (s *SyncController) pushSyncBucketQueue(peerID, bucket string) {
    s.syncBucketQueue = append(s.syncBucketQueue, []string{ peerID, bucket })
}

func (s *SyncController) peekSyncBucketQueue() []string {
    if len(s.syncBucketQueue) == 0 {
        return nil
    }

    return s.syncBucketQueue[0]
}

func (s *SyncController) popSyncBucketQueue() []string {
    if len(s.syncBucketQueue) == 0 {
        return nil
    }

    head := s.syncBucketQueue[0]

    s.syncBucketQueue = s.syncBucketQueue[1:]

    return head
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

    for _, bucket := range s.buckets.Incoming(peerID) {
        s.pushSyncBucketQueue(peerID, bucket.Name)
    }
    
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

    remaining := len(s.syncBucketQueue)

    for remaining != 0 {
        next := s.popSyncBucketQueue()

        if next[0] != peerID {
            s.pushSyncBucketQueue(next[0], next[1])
        }

        remaining--
    }

    s.mapMutex.Unlock()
    s.waitGroups[peerID].Wait()

    close(s.peers[peerID])
    delete(s.peers, peerID)
    delete(s.waitGroups, peerID)
}

func (s *SyncController) addResponderSession(peerID string, sessionID uint, bucketName string) bool {
    if !s.buckets.HasBucket(bucketName) {
        log.Errorf("Unable to add responder session %d for peer %s because %s is not a valid bucket name", sessionID, peerID, bucketName)
        
        return false
    } else if !s.buckets.Get(bucketName).ReplicationStrategy.ShouldReplicateOutgoing(peerID) {
        log.Errorf("Unable to add responder session %d for peer %s because bucket %s does not allow outgoing messages to this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.responderSessionsMap[peerID]; !ok {
        log.Errorf("Unable to add responder session %d for peer %s because this peer is not registered with the sync controller", sessionID, peerID)
        
        return false
    }
    
    if _, ok := s.responderSessionsMap[peerID][sessionID]; ok {
        log.Errorf("Unable to add responder session %d for peer %s because a responder session with this id for this peer already exists", sessionID, peerID)
        
        return false
    }
    
    newResponderSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper, 1),
        sender: s.peers[peerID],
        sessionState: NewResponderSyncSession(bucket),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    s.responderSessionsMap[peerID][sessionID] = newResponderSession
    newResponderSession.waitGroup.Add(1)
    
    select {
    case s.responderSessions <- newResponderSession:
        log.Infof("Added responder session %d for peer %s and bucket %s", sessionID, peerID, bucketName)
        
        return true
    default:
        log.Warningf("Unable to add responder session %d for peer %s and bucket %s because there are already %d responder sync sessions", sessionID, peerID, bucketName, s.maxSyncSessions)
        
        delete(s.responderSessionsMap[peerID], sessionID)
        newResponderSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) addInitiatorSession(peerID string, sessionID uint, bucketName string) bool {
    if !s.buckets.HasBucket(bucketName) {
        log.Errorf("Unable to add initiator session %d for peer %s because %s is not a valid bucket name", sessionID, peerID, bucketName)
        
        return false
    } else if !s.buckets.Get(bucketName).ReplicationStrategy.ShouldReplicateIncoming(peerID) {
        log.Errorf("Unable to add responder session %d for peer %s because bucket %s does not allow incoming messages from this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.initiatorSessionsMap[peerID]; !ok {
        log.Errorf("Unable to add initiator session %d for peer %s because this peer is not registered with the sync controller", sessionID, peerID)
        
        return false
    }
    
    newInitiatorSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper, 1),
        sender: s.peers[peerID],
        sessionState: NewInitiatorSyncSession(sessionID, bucket, s.explorationPathLimit, bucket.ReplicationStrategy.ShouldReplicateOutgoing(peerID)),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    // check map to see if it has this one
    if _, ok := s.initiatorSessionsMap[peerID][sessionID]; ok {
        log.Errorf("Unable to add initiator session %d for peer %s because an initiator session with this id for this peer already exists", sessionID, peerID)
        
        return false
    }
    
    s.initiatorSessionsMap[peerID][sessionID] = newInitiatorSession
    newInitiatorSession.waitGroup.Add(1)
    
    select {
    case s.initiatorSessions <- newInitiatorSession:
        log.Infof("Added initiator session %d for peer %s and bucket %s", sessionID, peerID, bucketName)
        
        s.popSyncBucketQueue()
        s.initiatorSessionsMap[peerID][sessionID].receiver <- nil
        
        return true
    default:
        log.Warningf("Unable to add initiator session %d for peer %s and bucket %s because there are already %d initiator sync sessions", sessionID, peerID, bucketName, s.maxSyncSessions)
        
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
    
    log.Infof("Removed responder session %d for peer %s", responderSession.sessionID, responderSession.peerID)
}

func (s *SyncController) removeInitiatorSession(initiatorSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.initiatorSessionsMap[initiatorSession.peerID]; ok {
        s.pushSyncBucketQueue(initiatorSession.peerID, initiatorSession.sessionState.(*InitiatorSyncSession).bucket.Name)
        delete(s.initiatorSessionsMap[initiatorSession.peerID], initiatorSession.sessionID)
    }
    
    s.mapMutex.Unlock()
    initiatorSession.waitGroup.Done()
    
    log.Infof("Removed initiator session %d for peer %s", initiatorSession.sessionID, initiatorSession.peerID)
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
        
        if !s.buckets.HasBucket(pushMessage.Bucket) {
            log.Errorf("Ignoring push message from %s because %s is not a valid bucket", nodeID, pushMessage.Bucket)
            
            return
        }
        
        if !s.buckets.Get(pushMessage.Bucket).ReplicationStrategy.ShouldReplicateIncoming(nodeID) {
            log.Errorf("Ignoring push message from %s because this node does not accept incoming pushes from bucket %s from that node", nodeID, pushMessage.Bucket)
            
            return
        }

        key := pushMessage.Key
        value := pushMessage.Value
        bucket := s.buckets.Get(pushMessage.Bucket)
        err := bucket.Node.Merge(map[string]*SiblingSet{ key: value })
        
        if err != nil {
            log.Errorf("Unable to merge object from peer %s into key %s in bucket %s: %v", nodeID, key, pushMessage.Bucket, err)
        } else {
            log.Infof("Merged object from peer %s into key %s in bucket %s", nodeID, key, pushMessage.Bucket)
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
                log.Warningf("[%s-%d] timeout", initiatorSession.peerID, initiatorSession.sessionID)
            }
            
            initialState := state.State()
            
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
            
            m.Direction = REQUEST
    
            if receivedMessage == nil {
                log.Debugf("[%s-%d] nil : (%s -> %s) : %s", initiatorSession.peerID, initiatorSession.sessionID, StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            } else {
                log.Debugf("[%s-%d] %s : (%s -> %s) : %s", initiatorSession.peerID, initiatorSession.sessionID, MessageTypeName(receivedMessage.MessageType), StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
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
                log.Warningf("[%s-%d] timeout", responderSession.peerID, responderSession.sessionID)
            }
            
            initialState := state.State()
            
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
        
            m.Direction = RESPONSE
            
            if receivedMessage == nil {
                log.Debugf("[%s-%d] nil : (%s -> %s) : %s", responderSession.peerID, responderSession.sessionID, StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            } else {
                log.Debugf("[%s-%d] %s : (%s -> %s) : %s", responderSession.peerID, responderSession.sessionID, MessageTypeName(receivedMessage.MessageType), StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
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
            time.Sleep(time.Millisecond * time.Duration(s.syncSessionPeriod))
            
            s.mapMutex.RLock()

            nextSyncBucket := s.peekSyncBucketQueue()

            if nextSyncBucket == nil {
                s.mapMutex.RUnlock()

                continue
            }

            peerID := nextSyncBucket[0]
            bucket := s.buckets.Get(nextSyncBucket[1])
    
            s.mapMutex.RUnlock()
            
            if s.addInitiatorSession(peerID, s.nextSessionID, bucket.Name) {
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

func (s *SyncController) BroadcastUpdate(bucket, key string, value *SiblingSet, n uint64) {    
    // broadcast the specified object to at most n peers, or all peers if n is non-positive
    var count uint64 = 0
    
    s.mapMutex.RLock()
    defer s.mapMutex.RUnlock()
    
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
    
    for peerID, w := range s.peers {
        if !s.buckets.Get(bucket).ReplicationStrategy.ShouldReplicateOutgoing(peerID) {
            continue
        }
        
        if n != 0 && count == n {
            break
        }

        log.Debugf("Push object at key %s to peer %s", key, peerID)
        w <- msg
        count += 1
    }
}
