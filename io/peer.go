package io

import (
    //"devicedb/dbobject"
    "github.com/gorilla/websocket"
    "sync"
    "errors"
)

type Peer struct {
    buckets *BucketList
    syncController *SyncController
}

func NewPeer(bucketList *BucketList) *Peer {
    peer := &Peer{
        buckets: bucketList,
    }
    
    return peer
}

func (peer *Peer) Accept(connection *websocket.Conn) {
    //peer.register("peerID", connection)
}

func (peer *Peer) Connect(url string) {
    //peer.register("peerID", connection)
}

func (peer *Peer) register(peerID string, connection *websocket.Conn) error {
    w := make(chan *SyncMessageWrapper)
    peer.syncController.addPeer(peerID, w)
    
    go func() {
        for {
            var nextMessage SyncMessageWrapper
            
            err := connection.ReadJSON(nextMessage)
            
            if err != nil {
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
    waitGroups map[string]chan *sync.WaitGroup
    initiatorSessionsMap map[string]map[uint]*InitiatorSyncSession
    responderSessionsMap map[string]map[uint]*ResponderSyncSession
    initiatorSessions chan *SyncSession
    responderSessions chan *SyncSession
    maxSyncSessions uint
    nextSessionID uint64
    mapMutex sync.RWMutex
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList) *SyncController {
    syncController := &SyncController{
        buckets: bucketList,
        incoming: make(chan *SyncMessageWrapper),
        peers: make(map[string]chan *SyncMessageWrapper),
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
    s.initiatorSessionsMap[peerID] = make(map[uint]*InitiatorSyncSession)
    s.responderSessionsMap[peerID] = make(map[uint]*ResponderSyncSession)
    
    return nil
}

func (s *SyncController) removePeer(peerID string) {
    s.mapMutex.Lock()
    
    if _, ok := s.peers[peerID]; !ok {
        return
    }
    
    for _, syncSession := range s.initiatorSessions[peerID] {
        close(syncSession.receiver)
    }
    
    for _, syncSession := range s.responderSessions[peerID] {
        close(syncSession.receiver)
    }
    
    s.mapMutex.Unlock()
    s.waitGroups[peerID].Wait()
    
    delete(s.peers, peerID)
    delete(s.waitGroups, peerID)
    delete(s.initiatorSessionsMap, peerID)
    delete(s.responderSessionsMap, peerID)
    
    wg.Wait()
}

func (s *SyncController) addResponderSession(peerID string, sessionID uint) bool {
    s.mapLock.Lock()
    defer s.mapLock.Unlock()
    
    newResponderSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper),
        sender: make(chan *SyncMessageWrapper),
        sessionState: NewResponderSyncSession(bucket),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    newResponderSession.waitGroup.Add(1)
    
    select {
    case s.responderSessions <- newResponderSession:
        return true
    default:
        newResponderSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) addInitiatorSession(peerID string, sessionID uint) bool {
    s.mapLock.Lock()
    defer s.mapLock.Unlock()
    
    newInitiatorSession := &SyncSession{
        receiver: make(chan *SyncMessageWrapper),
        sender: make(chan *SyncMessageWrapper),
        sessionState: NewInitiatorSyncSession(sessionID, bucket),
        waitGroup: s.waitGroups[peerID],
        peerID: peerID,
        sessionID: sessionID,
    }
    
    // check map to see if it has this one
    
    newInitiatorSession.waitGroup.Add(1)
    
    select {
    case s.initiatorSessions <- newInitiatorSession:
        return true
    default:
        newResponderSession.waitGroup.Done()
        return false
    }
}

func (s *SyncController) removeResponderSession(responderSession *SyncSession) {
    s.mapLock.Lock()
    
    
    
    s.mapLock.Unlock()
    responderSession.waitGroup.Done()
}

func (s *SyncController) removeInitiatorSession(initiatorSession *SyncSession) {
    s.mapLock.Lock()
    
    s.mapLock.Unlock()
    initiatorSession.waitGroup.Done()
}

func (s *SyncController) sendAbort(peerID string, sessionID uint) {
    s.peers[peerID] <- &SyncMessageWrapper{
        SessionID: sessionID,
        MessageType: SYNC_ABORT,
        MessageBody: &Abort{ },
    })
}

func (s *SyncController) receiveMessage(msg *SyncMessageWrapper) {
    nodeID := msg.nodeID
    sessionID := msg.SessionID
    
    if msg.Direction == REQUEST {
        if msg.MessageType == SYNC_START {
            s.mapLock.Lock()
            
            if !s.addResponderSession(nodeID, sessionID) {
                s.sendAbort(nodeID, sessionID)
            }
            
            s.mapLock.Unlock()
        }
        
        s.mapLock.RLock()
        defer s.mapLock.RUnlock()
        
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
        s.mapLock.RLock()
        defer s.mapLock.RUnlock()
        
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

func (s *SyncController) runInitiatorSession() {
    for initatorSession := range s.initiatorSessions {
        state := initiatorSession.sessionState.(InitiatorSyncSession)
        
        for receivedMessage := range initatorSession.receiver {
            var m *SyncMessageWrapper = state.NextState(msg)
                    
            if state.State() == END {
            }
            
            initiatorSession.sender <- m
        }
    
        s.removeInitiatorSession(initiatorSession)
    }
}

func (s *SyncController) runResponderSession() {
    for responderSession := range s.responderSessions {
        state := responderSession.sessionState.(ResponderSyncSession)
        
        for receivedMessage := range responderSession.receiver {
            var m *SyncMessageWrapper = state.NextState(msg)
                    
            if state.State() == END {
            }
            
            responderSession.sender <- m
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
    
    /*go func() {
        // wait X ms
        
    }*/
}

