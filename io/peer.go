package io

import (
    "bufio"
    //"devicedb/dbobject"
    "encoding/json"
    "github.com/gorilla/websocket"
)

type Peer struct {
    buckets *BucketList
}

func NewPeer(bucketList *BucketList) *Peer {
    peer := &Peer{
        buckets: bucketList,
    }
    
    return peer
}

func (peer *Peer) Accept(connection *websocket.Conn) {
    peer.register(peerID, connection)
}

func (peer *Peer) Connect(url string) {
    peer.register(peerID, connection)
}

func (peer *Peer) register(peerID string, connection *websocket.Conn) error {
    w := make(chan *SyncSession)
    err := peer.syncController.addPeer(peerID, w)
    
    if err != nil {
        return err
    }

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

type SyncController struct {
    buckets *BucketList
    incoming chan *SyncSession
    peers map[string]chan *SyncSession
    initiatorSessions map[string]map[string]*InitiatorSyncSession
    responderSessions map[string]map[string]*ResponderSyncSession
    maxSyncSessions uint
    nextSessionID uint64
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList) *SyncController {
    syncController := &SyncController{
        buckets: bucketList,
        incoming: make(chan *SyncSession),
        peers: make(map[string]chan *SyncSession),
        initiatorSessions: make(map[string]map[string]*InitiatorSyncSession),
        responderSessions: make(map[string]map[string]*ResponderSyncSession),
        maxSyncSessions: maxSyncSessions,
        nextSessionID: 1,
    }
    
    return syncController
}

func (s *SyncController) addPeer(peerID string, w chan *SyncSession) error {
}

func (s *SyncController) sendPeer(peerID string, msg *SyncSession) error {
}

func (s *SyncController) addResponderSession(peerID string, sessionID uint) bool {
}

func (s *SyncController) addInitiatorSession(peerID string, sessionID uint) bool {
}

func (s *SyncController) Start() {
    go func() {
        for msg := range incoming {
            nodeID := msg.nodeID
            sessionID := msg.SessionID
            dir := msg.Direction
            var m *SyncMessageWrapper
            
            if dir == REQUEST {
                if _, ok := s.responderSessions[nodeID][sessionID]; !ok {
                    if msg.MessageType == SYNC_START && s.addResponderSession(nodeID, sessionID) {
                        m = s.responderSessions[nodeID][sessionID].NextState(msg)
                    } else {
                        m = &SyncMessageWrapper{
                            SessionID: sessionID,
                            MessageType: SYNC_ABORT,
                            MessageBody: &Abort{ },
                        }
                    }
                } else {
                    m = s.responderSessions[nodeID][sessionID].NextState(msg)
                    
                    if s.responderSessions[nodeID][sessionID].State() == END {
                        // TODO destroy sync session
                    }
                }
                
                
                m.Direction = RESPONSE
            } else { // RESPONSE
                if _, ok := s.initiatorSessions[nodeID][sessionID]; !ok {
                    m = &SyncMessageWrapper{
                        SessionID: sessionID,
                        MessageType: SYNC_ABORT,
                        MessageBody: &Abort{ },
                    }
                } else {
                    m = s.initiatorSessions[nodeID][sessionID].NextState(msg)
                    
                    if s.initiatorSessions[nodeID][sessionID].State() == END {
                        // TODO destroy sync session
                    }
                }
                
                m.Direction = REQUEST
            }
        }
    }()
    
    go func() {
        // wait X ms
        
    }
}

