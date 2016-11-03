package io

import (
    "github.com/gorilla/websocket"
    "sync"
    "errors"
    "time"
    "math/rand"
    "crypto/tls"
    crand "crypto/rand"
    "fmt"
    "encoding/binary"
    "encoding/json"
    "strconv"
    "devicedb/dbobject"
)

const RECONNECT_WAIT_MAX_SECONDS = 32

func randomID() string {
    randomBytes := make([]byte, 16)
    crand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%016x%016x", high, low)
}

type Peer struct {
    syncController *SyncController
    tlsConfig *tls.Config
    peerMapLock sync.RWMutex
    peerMap map[string]*websocket.Conn
}

func NewPeer(syncController *SyncController, tlsConfig *tls.Config) *Peer {    
    peer := &Peer{
        syncController: syncController,
        tlsConfig: tlsConfig,
        peerMap: make(map[string]*websocket.Conn),
    }
    
    return peer
}

func (peer *Peer) Accept(connection *websocket.Conn) error {
    conn := connection.UnderlyingConn()
    
    if _, ok := conn.(*tls.Conn); ok {
        // https connection
        // extract peer id from common name
        peerID, err := peer.extractPeerID(conn.(*tls.Conn))
        
        if err != nil {
            log.Warningf("Unable to accept peer connection: %v", err)
            
            return err
        }
        
        err = peer.register(peerID, connection)
        
        // This indicates that the other side requested explicitly that this side is disconnected.
        // It was not an error and we should not try to reconnect
        if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
            log.Infof("Disconnected from peer %s", peerID)
        
            return err
        }
        
        if websocket.IsUnexpectedCloseError(err) {
            log.Warningf("Disconnected from peer %s: %v", peerID, err)
            
            return err
        }
        
        if err != nil {
            log.Warningf("Unable to register peer connection from %s: %v", peerID, err)
            
            return err
        }
        
        log.Infof("Accepted peer connection from %s", peerID)
    } else {
        // http connection
        err := peer.register(randomID(), connection)
        
        if err != nil {
            log.Warningf("Unable to register peer connection: %v", err)
            
            return err
        }
        
        log.Infof("Accepted peer connection")
    }
        
    return nil
}

func (peer *Peer) extractPeerID(conn *tls.Conn) (string, error) {
    // VerifyClientCertIfGiven
    verifiedChains := conn.ConnectionState().VerifiedChains
    
    if len(verifiedChains) != 1 {
        return "", errors.New("Invalid client certificate")
    }
    
    peerID := verifiedChains[0][0].Subject.CommonName
    
    return peerID, nil
}

func (peer *Peer) Connect(peerID, host string, port int) error {
    if peer.tlsConfig == nil {
        return errors.New("No tls config provided")
    }
    
    tlsConfig := *peer.tlsConfig
    tlsConfig.InsecureSkipVerify = false
    tlsConfig.ServerName = peerID
    
    dialer := &websocket.Dialer{
        TLSClientConfig: &tlsConfig,
    }

    go func() {
        reconnectWaitSeconds := 1
        
        for {
            conn, _, err := dialer.Dial("wss://" + host + ":" + strconv.Itoa(port) + "/sync", nil)
            
            if err != nil {
                log.Warningf("Unable to connect to %s on port %d: %v. Reconnecting in %ds...", host, port, err, reconnectWaitSeconds)
            
                time.Sleep(time.Second * time.Duration(reconnectWaitSeconds))
                
                if reconnectWaitSeconds != RECONNECT_WAIT_MAX_SECONDS {
                    reconnectWaitSeconds *= 2
                }
                
                continue
            }
            
            log.Infof("Connected to peer %s at %s on port %d", peerID, host, port)
            reconnectWaitSeconds = 1
            
            err = peer.register(peerID, conn)
            
            // This indicates that the other side requested explicitly that this side is disconnected.
            // It was not an error and we should not try to reconnect
            if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
                log.Infof("Disconnected from peer %s", peerID)
            
                break
            }
            
            if err != nil && !websocket.IsUnexpectedCloseError(err) {
                log.Warningf("Unable to register peer connection from %s: %v", peerID, err)
                
                break
            }
            
            // any non-standard close errors should result in a reconnect attempt
            log.Infof("Disconnected from peer %s. Reconnecting in %ds...", peerID, reconnectWaitSeconds)
            time.Sleep(time.Second * time.Duration(reconnectWaitSeconds))
            
            if reconnectWaitSeconds != RECONNECT_WAIT_MAX_SECONDS {
                reconnectWaitSeconds *= 2
            }
        }
    }()
    
    return nil
}

func (peer *Peer) Disconnect(peerID string) {
    peer.disconnect(peerID)
}

func (peer *Peer) disconnect(peerID string) {
    peer.peerMapLock.Lock()
    
    if _, ok := peer.peerMap[peerID]; ok {
        peer.peerMap[peerID].WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
    }
    
    delete(peer.peerMap, peerID)
    peer.peerMapLock.Unlock()
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
    }
    
    return err
}

func (peer *Peer) register(peerID string, connection *websocket.Conn) error {
    var result error = nil
    wg := sync.WaitGroup{ }
    
    w := make(chan *SyncMessageWrapper)
    err := peer.syncController.addPeer(peerID, w)
    
    wg.Add(2)
    
    if err != nil {
        log.Errorf("Unable to register peer %s: %v", peerID, err)
        
        return err
    }
    
    peer.peerMapLock.Lock()
    peer.peerMap[peerID] = connection
    peer.peerMapLock.Unlock()
    
    go func() {
        for {
            var nextRawMessage rawSyncMessageWrapper
            var nextMessage SyncMessageWrapper
            
            err := connection.ReadJSON(&nextRawMessage)
            
            if err != nil {
                log.Warningf("Unable to read from peer %s, unregistering peer: %v", peerID, err)
                
                peer.syncController.removePeer(peerID)
                peer.disconnect(peerID)
                
                result = err
                
                break
            }
            
            nextMessage.SessionID = nextRawMessage.SessionID
            nextMessage.MessageType = nextRawMessage.MessageType
            nextMessage.Direction = nextRawMessage.Direction
            
            err = peer.typeCheck(&nextRawMessage, &nextMessage)
            
            if err != nil {
                log.Warningf("Unable to read from peer %s, unregistering peer: %v", peerID, err)
                
                peer.syncController.removePeer(peerID)
                peer.disconnect(peerID)
                
                result = err
                
                break
            }
            
            nextMessage.nodeID = peerID
            
            peer.syncController.incoming <- &nextMessage
        }
        
        wg.Done()
    }()
    
    go func() {
        for msg := range w {
            err := connection.WriteJSON(msg)
            
            if err != nil {
                log.Warningf("Unable to write to peer %s: %v", peerID, err)
                    
                break
            }
        }
        
        peer.disconnect(peerID)
        
        wg.Done()
    }()
    
    wg.Wait()
    
    return result
}

func (peer *Peer) SyncController() *SyncController {
    return peer.syncController
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
    maxSyncSessions uint
    nextSessionID uint
    mapMutex sync.RWMutex
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList) *SyncController {
    syncController := &SyncController{
        buckets: bucketList,
        incoming: make(chan *SyncMessageWrapper),
        peers: make(map[string]chan *SyncMessageWrapper),
        waitGroups: make(map[string]*sync.WaitGroup),
        initiatorSessionsMap: make(map[string]map[uint]*SyncSession),
        responderSessionsMap: make(map[string]map[uint]*SyncSession),
        initiatorSessions: make(chan *SyncSession),
        responderSessions: make(chan *SyncSession),
        maxSyncSessions: maxSyncSessions,
        nextSessionID: 1,
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
    
    s.mapMutex.Unlock()
    s.waitGroups[peerID].Wait()

    close(s.peers[peerID])
    delete(s.peers, peerID)
    delete(s.waitGroups, peerID)
    delete(s.initiatorSessionsMap, peerID)
    delete(s.responderSessionsMap, peerID)
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
        receiver: make(chan *SyncMessageWrapper),
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
        receiver: make(chan *SyncMessageWrapper),
        sender: s.peers[peerID],
        sessionState: NewInitiatorSyncSession(sessionID, bucket),
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
    
    if _, ok := s.responderSessionsMap[responderSession.peerID]; !ok {
        log.Warningf("Cannot remove responder session %d for peer %d since it does not exist in the map", responderSession.sessionID, responderSession.peerID)
        
        s.mapMutex.Unlock()
        return
    }
    
    delete(s.responderSessionsMap[responderSession.peerID], responderSession.sessionID)
    
    s.mapMutex.Unlock()
    responderSession.waitGroup.Done()
    
    log.Infof("Removed responder session %d for peer %s", responderSession.sessionID, responderSession.peerID)
}

func (s *SyncController) removeInitiatorSession(initiatorSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.initiatorSessionsMap[initiatorSession.peerID]; !ok {
        log.Warningf("Cannot remove initiator session %d for peer %d since it does not exist in the map", initiatorSession.sessionID, initiatorSession.peerID)
        
        s.mapMutex.Unlock()
        return
    }
    
    delete(s.initiatorSessionsMap[initiatorSession.peerID], initiatorSession.sessionID)
    
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
        
        s.responderSessionsMap[nodeID][sessionID].receiver <- msg
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
        
        s.initiatorSessionsMap[nodeID][sessionID].receiver <- msg
    } else if msg.MessageType == SYNC_PUSH_MESSAGE {
        pushMessage := msg.MessageBody.(PushMessage)
        
        if !s.buckets.HasBucket(pushMessage.Bucket) {
            log.Errorf("Ignoring push message from %s because %s is not a valid bucket", nodeID, pushMessage.Bucket)
            
            return
        }

        key := pushMessage.Key
        value := pushMessage.Value
        bucket := s.buckets.Get(pushMessage.Bucket)
        err := bucket.Node.Merge(map[string]*dbobject.SiblingSet{ key: value })
        
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
        
        for receivedMessage := range initiatorSession.receiver {
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
        
        for receivedMessage := range responderSession.receiver {
            initialState := state.State()
            
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
        
            log.Debugf("[%s-%d] %s : (%s -> %s) : %s", responderSession.peerID, responderSession.sessionID, MessageTypeName(receivedMessage.MessageType), StateName(initialState), StateName(state.State()), MessageTypeName(m.MessageType))
            
            m.Direction = RESPONSE
            
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
            time.Sleep(time.Millisecond * 1000)
            
            s.mapMutex.RLock()
    
            var peerIndex int = 0
        
            if len(s.peers) > 0 {
                peerIndex = rand.Int() % len(s.peers)
            }
            
            peerID := ""
            
            for id, _ := range s.peers {
                if peerIndex == 0 {
                    peerID = id
                    
                    break
                }
                
                peerIndex -= 1
            }
            
            if len(peerID) == 0 {
                s.mapMutex.RUnlock()
                
                time.Sleep(time.Millisecond * 1000)
                
                continue
            }
            
            incoming := s.buckets.Incoming(peerID)
            
            if len(incoming) == 0 {
                s.mapMutex.RUnlock()
                
                time.Sleep(time.Millisecond * 1000)
                
                continue
            }
            
            bucket := incoming[rand.Int() % len(incoming)]
            
            s.mapMutex.RUnlock()
            
            if s.addInitiatorSession(peerID, s.nextSessionID, bucket.Name) {
                s.nextSessionID += 1
            } else {
            }
            
            time.Sleep(time.Millisecond * 1000)
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

func (s *SyncController) BroadcastUpdate(key string, value *dbobject.SiblingSet, n int) {
    // broadcast the specified object to at most n peers, or all peers if n is non-positive
    s.mapMutex.RLock()
    defer s.mapMutex.RUnlock()
    
    msg := &SyncMessageWrapper{
        SessionID: 0,
        MessageType: SYNC_PUSH_MESSAGE,
        MessageBody: PushMessage{
            Key: key,
            Value: value,
        },
        Direction: PUSH,
    }
    
    for peerID, w := range s.peers {
        if n <= 0 {
            break
        }
        
        select {
        case w <- msg:
            log.Debugf("Push object at key %s to peer %s", key, peerID)
            n -= 1
        default:
            log.Warningf("Failed to push object at key %s to peer %s", key, peerID)
        }
    }
}
