package server

import (
    . "devicedb/bucket"
    . "devicedb/partition"
    . "devicedb/site"
)

type SiteSyncSession struct {
    receiver chan *SyncMessageWrapper
    sender chan *SyncMessageWrapper
    sessionState interface{ }
    waitGroup *sync.WaitGroup
    peerID string
    sessionID uint
}

type SiteSyncController struct {
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

func NewSiteSyncController(maxSyncSessions uint, bucketList *BucketList, syncSessionPeriod uint64, explorationPathLimit uint32) *SyncController {
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
        s.pushSyncBucketQueue(peerID, bucket.Name())
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
        Log.Errorf("Unable to add responder session %d for peer %s because %s is not a valid bucket name", sessionID, peerID, bucketName)
        
        return false
    } else if !s.buckets.Get(bucketName).ShouldReplicateOutgoing(peerID) {
        Log.Errorf("Unable to add responder session %d for peer %s because bucket %s does not allow outgoing messages to this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
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
        sessionState: NewResponderSyncSession(bucket),
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
    if !s.buckets.HasBucket(bucketName) {
        Log.Errorf("Unable to add initiator session %d for peer %s because %s is not a valid bucket name", sessionID, peerID, bucketName)
        
        return false
    } else if !s.buckets.Get(bucketName).ShouldReplicateIncoming(peerID) {
        Log.Errorf("Unable to add responder session %d for peer %s because bucket %s does not allow incoming messages from this peer", sessionID, peerID, bucketName)
        
        return false
    }
    
    bucket := s.buckets.Get(bucketName)
    
    s.mapMutex.Lock()
    defer s.mapMutex.Unlock()
    
    if _, ok := s.initiatorSessionsMap[peerID]; !ok {
        Log.Errorf("Unable to add initiator session %d for peer %s because this peer is not registered with the sync controller", sessionID, peerID)
        
        return false
    }
    
    newInitiatorSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper, 1),
        sender: s.peers[peerID],
        sessionState: NewInitiatorSyncSession(sessionID, bucket, s.explorationPathLimit, bucket.ShouldReplicateOutgoing(peerID)),
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
        
        s.popSyncBucketQueue()
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
        s.pushSyncBucketQueue(initiatorSession.peerID, initiatorSession.sessionState.(*InitiatorSyncSession).bucket.Name())
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
        
        if !s.buckets.HasBucket(pushMessage.Bucket) {
            Log.Errorf("Ignoring push message from %s because %s is not a valid bucket", nodeID, pushMessage.Bucket)
            
            return
        }
        
        if !s.buckets.Get(pushMessage.Bucket).ShouldReplicateIncoming(nodeID) {
            Log.Errorf("Ignoring push message from %s because this node does not accept incoming pushes from bucket %s from that node", nodeID, pushMessage.Bucket)
            
            return
        }

        key := pushMessage.Key
        value := pushMessage.Value
        bucket := s.buckets.Get(pushMessage.Bucket)
        err := bucket.Merge(map[string]*SiblingSet{ key: value })
        
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
            
            if s.addInitiatorSession(peerID, s.nextSessionID, bucket.Name()) {
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
        if !s.buckets.Get(bucket).ShouldReplicateOutgoing(peerID) {
            continue
        }
        
        if n != 0 && count == n {
            break
        }

        Log.Debugf("Push object at key %s to peer %s", key, peerID)
        w <- msg
        count += 1
    }
}