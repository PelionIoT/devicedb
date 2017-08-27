package bucket

import (
    . "devicedb/data"
    . "devicedb/merkle"
)

type Bucket interface {
    Name() string
    ShouldReplicateOutgoing(peerID string) bool
    ShouldReplicateIncoming(peerID string) bool
    ShouldAcceptWrites(clientID string) bool
    ShouldAcceptReads(clientID string) bool
    RecordMetadata() error
    RebuildMerkleLeafs() error
    MerkleTree() *MerkleTree
    GarbageCollect(tombstonePurgeAge uint64) error
    Get(keys [][]byte) ([]*SiblingSet, error)
    GetMatches(keys [][]byte) (SiblingSetIterator, error)
    GetSyncChildren(nodeID uint32) (SiblingSetIterator, error)
    Forget(keys [][]byte) error
    Batch(batch *UpdateBatch) (map[string]*SiblingSet, error)
    Merge(siblingSets map[string]*SiblingSet) error
}
