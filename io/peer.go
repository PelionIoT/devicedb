package io

import (
    "bufio"
    "devicedb/dbobject"
)

type Peer struct {
    connections map[*websocket.Conn]bool
    buckets *BucketList
}

func NewPeer(bucketList *BucketList) *Peer {
    peer := &Peer{
        connections: make(map[*websocket.Conn]bool),
        buckets: bucketList,
    }
}

func (peer *Peer) Accept(connection *websocket.Conn) {
    // accept peer connection, start listening, negotiate handshake
}

func (peer *Peer) Connect(url string) {
    // establish a new connection with another peer
}

func (peer *Peer) register(connection *websocket.Conn) {
    reader := bufio.NewReader(connection)

    for {
        next, err := reader.ReadString('\n')
        
        if err != nil {
            
        }
        
        someChan <- next
    }
}

type SyncController struct {
    buckets *BucketList
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList) *SyncController {
    syncController := &SyncController{
        idleSyncSessions: make(chan *SyncSession, maxSyncSessions),
        buckets: bucketList,
    }
    
    return syncController
}

func (s *SyncController) Start() {
    go func() {
        for {
            syncSession := <-s.idleSyncSessions
            nextPeer := <-s.syncPeers
        }
    }()
    
    go func() {
        for {
            select {
            case sync := <-s.syncMessages:
            case join := <-s.joins:
                // peer join
            case leave := <-s.leaves:
                // peer leave
            }
        }
    }()
}

// the state machine
type InitiatorSyncSession struct {
    currentState int
    node uint32
    maxDepth uint32
}

func NewSyncSession(rootNode uint32, maxDepth uint32) {
    return &InitiatorSyncSession{ 0, rootNode, maxDepth }
}

func (syncSession *InitiatorSyncSession) NextState(syncMessageWrapper *SyncMessageWrapper) *SyncMessageWrapper {
    switch syncSession.currentState {
    case 0:
        syncSession.currentState = 1
        
        return &Start{
            ProtocolVersion: 1,
            MerkleDepth: 0,
            Bucket: "",
            SessionID: 0
        }
    case 1:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_START {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        }
        
        if syncSession.maxDepth > syncSession.MessageBody.(Start).MerkleDepth {
            syncSession.maxDepth = syncSession.MessageBody.(Start).MerkleDepth
        }
        
        // SEND ROOT HASH
        syncSession.currentState = 2
        
        return &MerkleNodeHash{
            SessionID: 0,
            NodeID: 0,
            HashHigh: 0,
            HashLow: 0
        }
    case 2:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        } else if syncSession.MessageBody.(MerkleNodeHash).HashHigh == myHashHigh && syncSession.MessageBody.(MerkleNodeHash).HashLow == myHashLow {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        } else {
            syncSession.currentState = 4
            
            return &MerkleNodeHash{
                SessionID: 0,
                NodeID: sync.LeftNode(syncSession.node),
                HashHigh: 0,
                HashLow: 0
            }
        }
    case 3:
        if depth(node) != syncSession.maxDepth {
            // SEND LEFT HASH
            syncSession.currentState = 4
            
            return
        } else {
            // SEND OBJECTS UNDER NODE
            syncSession.currentState = 6
        }
        
        return
    case 4:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        } else if HashHigh == myHashHigh && HashLow == myHashLow {
            syncSession.currentState = 5
            
            return &MerkleNodeHash{
                SessionID: 0,
                NodeID: sync.RightNode(syncSession.node),
                HashHigh: 0,
                HashLow: 0
            }
        } else if depth(node) != maxDepth {
            syncSession.node = sync.LeftNode(syncSession.node)
            
            return &MerkleNodeHash{
                SessionID: 0,
                NodeID: sync.LeftNode(syncSession.node),
                HashHigh: 0,
                HashLow: 0
            }
        } else {
            syncSession.currentState = 6
            
            return &ObjectHash{
                NodeID: syncSession.node
            }
        }
    case 5:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        } else if HashHigh == myHashHigh && HashLow == myHashLow {
            syncSession.currentState = 7
            
            return &Abort{
                SessionID: 0
            }
        } else if depth(node) != maxDepth {
            syncSession.node = sync.RightNode(syncSession.node)
            
            return &MerkleNodeHash{
                SessionID: 0,
                NodeID: sync.LeftNode(syncSession.node),
                HashHigh: 0,
                HashLow: 0
            }
        } else {
            syncSession.currentState = 6
            
            return &ObjectHash{
                NodeID: syncSession.node
            }
        }
    case 6:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_PUSH_MESSAGE {
            syncSession.currentState = 7
            
            return nil
        } else {
            // TODO push to DATABASE
            return nil
        }
    case 7:
        return nil
    }
}

// Start -> Root Compare -> Tree Walk -> Key Scan -> End
// Error

// ------
// |    |


// THE PROTOCOL
// [Type (1)]['\n']
// []['\n']
// types -> 

const (
    SYNC_START = iota
    SYNC_ABORT = iota
    SYNC_NODE_HASH = iota
    SYNC_PUSH_MESSAGE = iota
)

type SyncMessageWrapper struct {
    MessageType uint `json:"type"`
    MessageBody interface{ } `json:"body"`
}

func NewSyncMessage(messageType uint, messageBody interface{ }) *SyncMessageWrapper {
    return &SyncMessageWrapper{ messageType, messageBody }
}

type Start struct {
    ProtocolVersion uint
    MerkleDepth uint
    Bucket string
    SessionID uint
}

type Abort struct {
    SessionID uint
}

type MerkleNodeHash struct {
    SessionID uint
    NodeID uint32
    HashHigh uint64
    HashLow uint64
}

type ObjectHash struct {
    SessionID uint
    Key string
    HashHigh uint64
    HashLow uint64
}

type PushMessage struct {
    SessionID uint
    Key string
    Value dbobject.SiblingSet
}
