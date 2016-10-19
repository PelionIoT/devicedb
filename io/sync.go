package io

import (
    "devicedb/dbobject"
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

const PROTOCOL_VERSION uint = 1

// the state machine
type InitiatorSyncSession struct {
    sessionID uint
    currentState int
    node uint32
    maxDepth uint8
    theirDepth uint8
    bucket Bucket
}

func NewInitiatorSyncSession(id uint, bucket Bucket) *InitiatorSyncSession {
    return &InitiatorSyncSession{
        sessionID: id,
        currentState: 0,
        node: bucket.Node.MerkleTree().RootNode(),
        maxDepth: bucket.Node.MerkleTree().Depth(),
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

func (syncSession *InitiatorSyncSession) SetCurrentNode(n uint32) {
    syncSession.node = n
}

func (syncSession *InitiatorSyncSession) CurrentNode() uint32 {
    return syncSession.node
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
    
        return &SyncMessageWrapper{
            SessionID: syncSession.sessionID,
            MessageType: SYNC_NODE_HASH,
            MessageBody: MerkleNodeHash{
                NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.node, syncSession.theirDepth),
                HashHigh: syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.node).High(),
                HashLow: syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.node).Low(),
            },
        }
    case ROOT_HASH_COMPARE:
        myHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.node)
        
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
        } else if syncSession.bucket.Node.MerkleTree().Level(syncSession.node) != syncSession.maxDepth {
            syncSession.currentState = LEFT_HASH_COMPARE
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node), syncSession.theirDepth),
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
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.node, syncSession.theirDepth),
                },
            }
        }
    case LEFT_HASH_COMPARE:
        myLeftChildHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node))
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncMessageWrapper.MessageBody.(MerkleNodeHash).HashHigh == myLeftChildHash.High() && syncMessageWrapper.MessageBody.(MerkleNodeHash).HashLow == myLeftChildHash.Low() {
            syncSession.currentState = RIGHT_HASH_COMPARE
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().RightChild(syncSession.node), syncSession.theirDepth),
                    HashHigh: 0,
                    HashLow: 0,
                },
            }
        } else if syncSession.bucket.Node.MerkleTree().Level(syncSession.node) != syncSession.maxDepth - 1 {
            syncSession.node = syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node)
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node), syncSession.theirDepth),
                    HashHigh: 0,
                    HashLow: 0,
                },
            }
        } else {
            syncSession.node = syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node)
            syncSession.currentState = DB_OBJECT_PUSH
                
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.node, syncSession.theirDepth),
                },
            }
        }
    case RIGHT_HASH_COMPARE:
        myRightChildHash := syncSession.bucket.Node.MerkleTree().NodeHash(syncSession.bucket.Node.MerkleTree().RightChild(syncSession.node))
        
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_NODE_HASH {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncMessageWrapper.MessageBody.(MerkleNodeHash).HashHigh == myRightChildHash.High() && syncMessageWrapper.MessageBody.(MerkleNodeHash).HashLow == myRightChildHash.Low() {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else if syncSession.bucket.Node.MerkleTree().Level(syncSession.node) != syncSession.maxDepth - 1 {
            syncSession.node = syncSession.bucket.Node.MerkleTree().RightChild(syncSession.node)
            syncSession.currentState = LEFT_HASH_COMPARE
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_NODE_HASH,
                MessageBody: MerkleNodeHash{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.bucket.Node.MerkleTree().LeftChild(syncSession.node), syncSession.theirDepth),
                    HashHigh: 0,
                    HashLow: 0,
                },
            }
        } else {
            syncSession.node = syncSession.bucket.Node.MerkleTree().RightChild(syncSession.node)
            syncSession.currentState = DB_OBJECT_PUSH
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.node, syncSession.theirDepth),
                },
            }
        }
    case DB_OBJECT_PUSH:
        if syncMessageWrapper == nil || syncMessageWrapper.MessageType != SYNC_PUSH_MESSAGE {
            syncSession.currentState = END
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_ABORT,
                MessageBody: Abort{ },
            }
        } else {
            // TODO push to DATABASE
            
            return &SyncMessageWrapper{
                SessionID: syncSession.sessionID,
                MessageType: SYNC_OBJECT_NEXT,
                MessageBody: ObjectNext{
                    NodeID: syncSession.bucket.Node.MerkleTree().TranslateNode(syncSession.node, syncSession.theirDepth),
                },
            }
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
    iter *SiblingSetIterator
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
        } else {
            // if items to iterate over, send first
            nodeID := syncMessageWrapper.MessageBody.(ObjectNext).NodeID
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
                iter.Release()
                
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
        
        if syncSession.iter == nil || !syncSession.iter.Next() {
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
)

type SyncMessageWrapper struct {
    SessionID uint `json:"sessionID"`
    MessageType int `json:"type"`
    MessageBody interface{ } `json:"body"`
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
    Key string
    Value *dbobject.SiblingSet
}