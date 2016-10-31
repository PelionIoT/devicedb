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
    "strconv"
)

func randomID() string {
    randomBytes := make([]byte, 16)
    crand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%016x%016x", high, low)
}

type Peer struct {
    syncController *SyncController
    dialer *websocket.Dialer
}

func NewPeer(syncController *SyncController, tlsConfig *tls.Config) *Peer {
    dialer := &websocket.Dialer{
        TLSClientConfig: tlsConfig,
    }
    
    peer := &Peer{
        syncController: syncController,
        dialer: dialer,
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
            log.Errorf("Unable to accept peer connection: %v", err)
            
            return err
        }
        
        err = peer.register(peerID, connection)
        
        if err != nil {
            log.Errorf("Unable to register peer connection from %s: %v", peerID, err)
            
            return err
        }
        
        log.Infof("Accepted peer connection from %s", peerID)
    } else {
        // http connection
        err := peer.register(randomID(), connection)
        
        if err != nil {
            log.Errorf("Unable to register peer connection: %v", err)
            
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

func (peer *Peer) Connect(host string, port int) error {
    //url := host:" + strconv.Itoa(server.Port()))
    conn, _, err := peer.dialer.Dial("wss://" + host + ":" + strconv.Itoa(port) + "/sync", nil)
    
    if err != nil {
        log.Errorf("Unable to connect to %s on port %d: %v", host, port, err)
        
        return err
    }
    
    log.Infof("Connected to %s on port %d: %v", host, port, conn)
    
    return nil
}

func (peer *Peer) register(peerID string, connection *websocket.Conn) error {
    w := make(chan *SyncMessageWrapper)
    peer.syncController.addPeer(peerID, w)
    
    go func() {
        for {
            var nextMessage SyncMessageWrapper
            
            err := connection.ReadJSON(nextMessage)
            
            if err != nil {
                peer.syncController.removePeer(peerID)
                
                break
            }
            
            nextMessage.nodeID = peerID
            
            peer.syncController.incoming <- &nextMessage
        }
    }()
    
    go func() {
        for msg := range w {
            err := connection.WriteJSON(msg)
            
            if err != nil {
                break
            }
        }
    }()
    
    return nil
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
        return false
    } else if !s.buckets.Get(bucketName).ReplicationStrategy.ShouldReplicateOutgoing(peerID) {
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.responderSessionsMap[peerID]; !ok {
        return false
    }
    
    newResponderSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper),
        sender: make(chan *SyncMessageWrapper),
        sessionState: NewResponderSyncSession(bucket),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    s.responderSessionsMap[peerID][sessionID] = newResponderSession
    newResponderSession.waitGroup.Add(1)
    
    if _, ok := s.responderSessionsMap[peerID][sessionID]; ok {
        return false
    }
    
    select {
    case s.responderSessions <- newResponderSession:
        return true
    default:
        delete(s.responderSessionsMap[peerID], sessionID)
        newResponderSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) addInitiatorSession(peerID string, sessionID uint, bucketName string) bool {
    if !s.buckets.HasBucket(bucketName) {
        return false
    } else if !s.buckets.Get(bucketName).ReplicationStrategy.ShouldReplicateIncoming(peerID) {
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.initiatorSessionsMap[peerID]; !ok {
        return false
    }
    
    newInitiatorSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper),
        sender: make(chan *SyncMessageWrapper),
        sessionState: NewInitiatorSyncSession(sessionID, bucket),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    // check map to see if it has this one
    if _, ok := s.initiatorSessionsMap[peerID][sessionID]; ok {
        return false
    }
    
    s.initiatorSessionsMap[peerID][sessionID] = newInitiatorSession
    newInitiatorSession.waitGroup.Add(1)
    
    select {
    case s.initiatorSessions <- newInitiatorSession:
        return true
    default:
        delete(s.initiatorSessionsMap[peerID], sessionID)
        newInitiatorSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) removeResponderSession(responderSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.responderSessionsMap[responderSession.peerID]; !ok {
        s.mapMutex.Unlock()
        return
    }
    
    delete(s.responderSessionsMap[responderSession.peerID], responderSession.sessionID)
    
    s.mapMutex.Unlock()
    responderSession.waitGroup.Done()
}

func (s *SyncController) removeInitiatorSession(initiatorSession *SyncSession) {
    s.mapMutex.Lock()
    
    if _, ok := s.initiatorSessionsMap[initiatorSession.peerID]; !ok {
        s.mapMutex.Unlock()
        return
    }
    
    delete(s.initiatorSessionsMap[initiatorSession.peerID], initiatorSession.sessionID)
    
    s.mapMutex.Unlock()
    initiatorSession.waitGroup.Done()
}

func (s *SyncController) sendAbort(peerID string, sessionID uint) {
    s.peers[peerID] <- &SyncMessageWrapper{
        SessionID: sessionID,
        MessageType: SYNC_ABORT,
        MessageBody: &Abort{ },
    }
}

func (s *SyncController) receiveMessage(msg *SyncMessageWrapper) {
    nodeID := msg.nodeID
    sessionID := msg.SessionID
    
    if !s.typeCheck(msg) {
        s.sendAbort(nodeID, sessionID)
        
        return
    }
    
    if msg.Direction == REQUEST {
        if msg.MessageType == SYNC_START {
            s.mapMutex.Lock()
            
            if !s.addResponderSession(nodeID, sessionID, msg.MessageBody.(Start).Bucket) {
                s.sendAbort(nodeID, sessionID)
            }
            
            s.mapMutex.Unlock()
        }
        
        s.mapMutex.RLock()
        defer s.mapMutex.RUnlock()
        
        if _, ok := s.responderSessionsMap[nodeID]; !ok {
            s.sendAbort(nodeID, sessionID)
            
            return
        }
        
        if _, ok := s.responderSessionsMap[nodeID][sessionID]; !ok {
            s.sendAbort(nodeID, sessionID)
            
            return
        }
        
        s.responderSessionsMap[nodeID][sessionID].receiver <- msg
    } else { // RESPONSE
        s.mapMutex.RLock()
        defer s.mapMutex.RUnlock()
        
        if _, ok := s.initiatorSessionsMap[nodeID]; !ok {
            s.sendAbort(nodeID, sessionID)
            
            return
        }
        
        if _, ok := s.initiatorSessionsMap[nodeID][sessionID]; !ok {
            s.sendAbort(nodeID, sessionID)
            
            return
        }
        
        s.initiatorSessionsMap[nodeID][sessionID].receiver <- msg
    }
}

func (s *SyncController) typeCheck(msg *SyncMessageWrapper) bool {
    ok := false
    
    switch msg.MessageType {
    case SYNC_START:
        _, ok = msg.MessageBody.(Start)
    case SYNC_ABORT:
        _, ok = msg.MessageBody.(Abort)
    case SYNC_NODE_HASH:
        _, ok = msg.MessageBody.(MerkleNodeHash)
    case SYNC_OBJECT_NEXT:
        _, ok = msg.MessageBody.(MerkleNodeHash)
    case SYNC_PUSH_MESSAGE:
        _, ok = msg.MessageBody.(PushMessage)
    }
    
    return ok
}

func (s *SyncController) runInitiatorSession() {
    for initiatorSession := range s.initiatorSessions {
        state := initiatorSession.sessionState.(InitiatorSyncSession)
        
        for receivedMessage := range initiatorSession.receiver {
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
        
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
        state := responderSession.sessionState.(ResponderSyncSession)
        
        for receivedMessage := range responderSession.receiver {
            var m *SyncMessageWrapper = state.NextState(receivedMessage)
        
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

func (s *SyncController) Start() {
    for i := 0; i < int(s.maxSyncSessions); i += 1 {
        go s.runInitiatorSession()
        go s.runResponderSession()
    }
    
    go func() {
        // multiplex incoming messages accross sync sessions
        for msg := range s.incoming {
            s.receiveMessage(msg)
        }
    }()
    
    go func() {
        for {
            time.Sleep(time.Millisecond * 1000)
            
            s.mapMutex.RLock()
            defer s.mapMutex.RUnlock()
            
            peerIndex := rand.Int() % len(s.peers)
            peerID := ""
            
            for id, _ := range s.peers {
                if peerIndex == 0 {
                    peerID = id
                    
                    break
                }
                
                peerIndex -= 1
            }
            
            if len(peerID) == 0 {
                continue
            }
            
            incoming := s.buckets.Incoming(peerID)
            
            if len(incoming) == 0 {
                continue
            }
            
            bucket := incoming[rand.Int() % len(incoming)]
            
            if s.addInitiatorSession(peerID, s.nextSessionID, bucket.Name) {
                s.nextSessionID += 1
            } else {
                
            }
        }
    }()
}

