package devicedb

import (
    "encoding/json"
)

const (
    START = iota
    HANDSHAKE = iota
    ROOT_HASH_COMPARE = iota
    LEFT_HASH_COMPARE = iota
    RIGHT_HASH_COMPARE = iota
    HASH_COMPARE = iota
    DB_OBJECT_PUSH = iota
    END = iota
)

func StateName(s int) string {
    names := map[int]string{
        START: "START",
        HANDSHAKE: "HANDSHAKE",
        ROOT_HASH_COMPARE: "ROOT_HASH_COMPARE",
        LEFT_HASH_COMPARE: "LEFT_HASH_COMPARE",
        RIGHT_HASH_COMPARE: "RIGHT_HASH_COMPARE",
        HASH_COMPARE: "HASH_COMPARE",
        DB_OBJECT_PUSH: "DB_OBJECT_PUSH",
        END: "END",
    }
    
    return names[s]
}

const PROTOCOL_VERSION uint = 2

// the state machine
type InitiatorSyncSession struct {
    sessionID uint
    currentState int
    maxDepth uint8
    theirDepth uint8
    explorationQueue []uint32
    explorationPathLimit uint32
    bucket Bucket
}

func NewInitiatorSyncSession(id uint, bucket Bucket, explorationPathLimit uint32) *InitiatorSyncSession {
    return &InitiatorSyncSession{
        sessionID: id,
        currentState: START,
        maxDepth: bucket.Node.MerkleTree().Depth(),
        explorationQueue: make([]uint32, 0),
        explorationPathLimit: explorationPathLimit,
        bucket: bucket,
    }
}

func (syncSession *InitiatorSyncSession) State() int {
    return syncSession.currentState
}

func (syncSession *InitiatorSyncSession) SetState(state int) {
    syncSession.currentState = state
}

func (syncSession *InitiatorSyncSession) SetResponderDepth(d uint8) {
    if d < syncSession.maxDepth {
        syncSession.maxDepth = d
    }
    
    syncSession.theirDepth = d
}

func (syncSession *InitiatorSyncSession) ResponderDepth() uint8 {
    return syncSession.theirDepth
}

func (syncSession *InitiatorSyncSession) PushExplorationQueue(n uint32) {
    syncSession.explorationQueue = append(syncSession.explorationQueue, n)
}

func (syncSession *InitiatorSyncSession) PopExplorationQueue() uint32 {
    var n uint32

    if len(syncSession.explorationQueue) != 0 {
        n = syncSession.explorationQueue[0]

        syncSession.explorationQueue = syncSession.explorationQueue[1:]
    }

    return n
}

func (syncSession *InitiatorSyncSession) PeekExplorationQueue() uint32 {
    var n uint32

    if len(syncSession.explorationQueue) != 0 {
        n = syncSession.explorationQueue[0]
    }

    return n
}

func (syncSession *InitiatorSyncSession) ExplorationQueueSize() uint32 {
    return uint32(len(syncSession.explorationQueue))
}

func (syncSession *InitiatorSyncSession) SetExplorationPathLimit(limit uint32) {
    syncSession.explorationPathLimit = limit
}

func (syncSession *InitiatorSyncSession) ExplorationPathLimit() uint32 {
    return syncSession.explorationPathLimit
}

func (syncSession *InitiatorSyncSession) NextState(syncMessageWrapper *SyncMessageWrapper) *SyncMessageWrapper {
    switch syncSession.currentState {
    case START:
        syncSession.currentState = HANDSHAKE
    
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_START,
            MessageBody: Start{
                ProtocolVersion: PROTOCOL_VERSION,
                MerkleDepth: syncSession.bucket.Node.MerkleTree().Depth(),
                Bucket: syncSession.bucket.Name,
            },
        }
    case HANDSHAKE:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_START {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: &Abort{ },
            }
        }
        
        if syncSession.maxDepth > syncMessageWrapper.MessageBody.(Start).MerkleDepth {
            syncSession.maxDepth = syncMessageWrapper.MessageBody.(Start).MerkleDepth
        }
        
        syncSession.theirDepth = syncMessageWrapper.MessageBody.(Start).MerkleDepth
        syncSession.currentState = ROOT_HASH_COMPARE
        syncSession.PushExplorationQueue(syncSession.bucket.Node.MerkleTree().RootNode())
    
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_NODE_HASH,
            MessageBody: MerkleNodeHash{
                NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.PeekExplorationQueue(), syncSession.theirDepth),
                HashHigh: syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.PeekExplorationQueue()).High(),
                HashLow: syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.PeekExplorationQueue()).Low(),
            },
        }
    case ROOT_HASH_COMPARE:
        myHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.PeekExplorationQueue())
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncMessageWrapper.MessageBody.(MerkleNodeHash).HashHigh == myHash.High() && syncMessageWrapper.MessageBody.(MerkleNodeHash).HashLow == myHash.Low() {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncSession.bucket.Node.MerkleTree().Level(syncSession.PeekExplorationQueue()) != syncSession.maxDepth {
            syncSession.currentState = LEFT_HASH_COMPARE
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.PeekExplorationQueue()), syncSession.theirDepth),
                    HashHigh: 0,
                    HashLow: 0,
                },
            }
        } else {
            syncSession.currentState = DB_OBJECT_PUSH
                
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.PeekExplorationQueue(), syncSession.theirDepth),
                },
            }
        }
    case LEFT_HASH_COMPARE:
        myLeftChildHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.PeekExplorationQueue()))
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
        
        if syncMessageWrapper.MessageBody.(MerkleNodeHash).HashHigh != myLeftChildHash.High() || syncMessageWrapper.MessageBody.(MerkleNodeHash).HashLow != myLeftChildHash.Low() {
            syncSession.PushExplorationQueue(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.PeekExplorationQueue()))
        }

        syncSession.currentState = RIGHT_HASH_COMPARE
        
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_NODE_HASH,
            MessageBody: MerkleNodeHash{
                NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().RightChild(syncSession.PeekExplorationQueue()), syncSession.theirDepth),
                HashHigh: 0,
                HashLow: 0,
            },
        }
    case RIGHT_HASH_COMPARE:
        myRightChildHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.bucket.Node.MerkleTree().RightChild(syncSession.PeekExplorationQueue()))
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }

        if syncMessageWrapper.MessageBody.(MerkleNodeHash).HashHigh != myRightChildHash.High() || syncMessageWrapper.MessageBody.(MerkleNodeHash).HashLow != myRightChildHash.Low() {
            if syncSession.ExplorationQueueSize() <= syncSession.ExplorationPathLimit() {
                syncSession.PushExplorationQueue(syncSession.bucket.Node.MerkleTree().RightChild(syncSession.PeekExplorationQueue()))
            }
        }

        syncSession.PopExplorationQueue()

        if syncSession.ExplorationQueueSize() == 0 {
            // no more nodes to explore. abort
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncSession.bucket.Node.MerkleTree().Level(syncSession.PeekExplorationQueue()) == syncSession.maxDepth {
            // cannot dig any deeper in any paths. need to move to DB_OBJECT_PUSH
            syncSession.currentState = DB_OBJECT_PUSH

            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.PeekExplorationQueue(), syncSession.theirDepth),
                },
            }
        }

        syncSession.currentState = LEFT_HASH_COMPARE

        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_NODE_HASH,
            MessageBody: MerkleNodeHash{
                NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.PeekExplorationQueue()), syncSession.theirDepth),
                HashHigh: 0,
                HashLow: 0,
            },
        }
    case DB_OBJECT_PUSH:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_PUSH_MESSAGE && syncMessageWrapper.MessageType != SYNC_PUSH_DONE {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }

        if syncMessageWrapper.MessageType == SYNC_PUSH_DONE {
            syncSession.PopExplorationQueue()

            if syncSession.ExplorationQueueSize() == 0 {
                syncSession.currentState = END

                return &SyncMessageWrapper{
                    SessionID: syncSession.sessionID,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                }
            }

            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.PeekExplorationQueue(), syncSession.theirDepth),
                },
            }
        }

        var key string = syncMessageWrapper.MessageBody.(PushMessage).Key
        var siblingSet *SiblingSet = syncMessageWrapper.MessageBody.(PushMessage).Value

        err := syncSession.bucket.Node.Merge(map[string]*SiblingSet{ key: siblingSet })
        
        if err != nil {
            syncSession.currentState = END
        
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
        
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_OBJECT_NEXT,
            MessageBody: ObjectNext{
                NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.PeekExplorationQueue(), syncSession.theirDepth),
            },
        }
    case END:
        return nil
    }
    
    return nil
}

    // the state machine
type ResponderSyncSession struct {
    sessionID uint
    currentState int
    node uint32
    maxDepth uint8
    theirDepth uint8
    bucket Bucket
    iter *MerkleChildrenIterator
    currentIterationNode uint32
}

func NewResponderSyncSession(bucket Bucket) *ResponderSyncSession {
    return &ResponderSyncSession{
        sessionID: 0,
        currentState: START,
        node: bucket.Node.MerkleTree().RootNode(),
        maxDepth: bucket.Node.MerkleTree().Depth(),
        bucket: bucket,
        iter: nil,
    }
}

func (syncSession *ResponderSyncSession) State() int {
    return syncSession.currentState
}

func (syncSession *ResponderSyncSession) SetState(state int) {
    syncSession.currentState = state
}

func (syncSession *ResponderSyncSession) SetInitiatorDepth(d uint8) {
    syncSession.theirDepth = d
}

func (syncSession *ResponderSyncSession) InitiatorDepth() uint8 {
    return syncSession.theirDepth
}

func (syncSession *ResponderSyncSession) NextState(syncMessageWrapper *SyncMessageWrapper) *SyncMessageWrapper {
    switch syncSession.currentState {
    case START:
        if syncMessageWrapper != nil {
            syncSession.sessionID = syncMessageWrapper.SessionID
        }
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_START {
            syncSession.currentState = END
        
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
    
        syncSession.theirDepth = syncMessageWrapper.MessageBody.(Start).MerkleDepth
        syncSession.currentState = HASH_COMPARE
    
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_START,
            MessageBody: Start{
                ProtocolVersion: PROTOCOL_VERSION,
                MerkleDepth: syncSession.bucket.Node.MerkleTree().Depth(),
                Bucket: syncSession.bucket.Name,
            },
        }
    case HASH_COMPARE:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH && syncMessageWrapper.MessageType != SYNC_OBJECT_NEXT {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
        
        if syncMessageWrapper.MessageType == SYNC_NODE_HASH {
            nodeID := syncMessageWrapper.MessageBody.(MerkleNodeHash).NodeID
            
            if nodeID >= syncSession.bucket.Node.MerkleTree().NodeLimit() || nodeID == 0 {
                syncSession.currentState = END
                
                return &SyncMessageWrapper{
                    SessionID: syncSession.sessionID,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                }
            }
            
            nodeHash := syncSession.bucket.Node.MerkleTree().NodeHash(nodeID)
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(nodeID, syncSession.theirDepth),
                    HashHigh: nodeHash.High(),
                    HashLow: nodeHash.Low(),
                },
            }
        }

        // if items to iterate over, send first
        nodeID := syncMessageWrapper.MessageBody.(ObjectNext).NodeID
        syncSession.currentIterationNode = nodeID
        iter, err := syncSession.bucket.Node.GetSyncChildren(nodeID)
        
        if err != nil {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
        
        if !iter.Next() {
            err := iter.Error()
            iter.Release()

            if err == nil {
                syncSession.currentState = DB_OBJECT_PUSH

                return &SyncMessageWrapper{
                    SessionID: syncSession.sessionID,
                    MessageType: SYNC_PUSH_DONE,
                    MessageBody: PushDone{ },
                }
            }
            
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
    
        syncSession.iter = iter
        syncSession.currentState = DB_OBJECT_PUSH
        
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_PUSH_MESSAGE,
            MessageBody: PushMessage{
                Key: string(iter.Key()),
                Value: iter.Value(),
            },
        }
    case DB_OBJECT_PUSH:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_OBJECT_NEXT {
            if syncSession.iter != nil {
                syncSession.iter.Release()
            }
                
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }

        if syncSession.iter == nil {
            syncSession.currentState = END
                
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }

        if syncSession.currentIterationNode != syncMessageWrapper.MessageBody.(ObjectNext).NodeID {
            if syncSession.iter != nil {
                syncSession.iter.Release()
            }

            syncSession.currentIterationNode = syncMessageWrapper.MessageBody.(ObjectNext).NodeID
            iter, err := syncSession.bucket.Node.GetSyncChildren(syncSession.currentIterationNode)

            if err != nil {
                syncSession.currentState = END
                
                return &SyncMessageWrapper{
                    SessionID: syncSession.sessionID,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                }
            }

            syncSession.iter = iter
        }
        
        if !syncSession.iter.Next() {
            if syncSession.iter != nil {
                err := syncSession.iter.Error()

                syncSession.iter.Release()

                if err == nil {
                    return &SyncMessageWrapper{
                        SessionID: syncSession.sessionID,
                        MessageType: SYNC_PUSH_DONE,
                        MessageBody: PushDone{ },
                    }
                }
            }
            
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        }
        
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_PUSH_MESSAGE,
            MessageBody: PushMessage{
                Key: string(syncSession.iter.Key()),
                Value: syncSession.iter.Value(),
            },
        }
    case END:
        return nil
    }
    
    return nil
}

const (
    SYNC_START = iota
    SYNC_ABORT = iota
    SYNC_NODE_HASH = iota
    SYNC_OBJECT_NEXT = iota
    SYNC_PUSH_MESSAGE = iota
    REQUEST = iota
    RESPONSE = iota
    PUSH = iota
    SYNC_PUSH_DONE = iota
)

func MessageTypeName(m int) string {
    names := map[int]string{
        SYNC_START: "SYNC_START",
        SYNC_ABORT: "SYNC_ABORT",
        SYNC_NODE_HASH: "SYNC_NODE_HASH",
        SYNC_OBJECT_NEXT: "SYNC_OBJECT_NEXT",
        SYNC_PUSH_MESSAGE: "SYNC_PUSH_MESSAGE",
        SYNC_PUSH_DONE: "SYNC_PUSH_DONE",
    }
    
    return names[m]
}

type rawSyncMessageWrapper struct {
    SessionID uint `json:"sessionID"`
    MessageType int `json:"type"`
    MessageBody json.RawMessage `json:"body"`
    Direction uint `json:"dir"`
    nodeID string
}

type SyncMessageWrapper struct {
    SessionID uint `json:"sessionID"`
    MessageType int `json:"type"`
    MessageBody interface{ } `json:"body"`
    Direction uint `json:"dir"`
    nodeID string
}

type Start struct {
    ProtocolVersion uint
    MerkleDepth uint8
    Bucket string
}

type Abort struct {
}

type MerkleNodeHash struct {
    NodeID uint32
    HashHigh uint64
    HashLow uint64
}

type ObjectNext struct {
    NodeID uint32
}

type PushMessage struct {
    Bucket string
    Key string
    Value *SiblingSet
}

type PushDone struct {
}