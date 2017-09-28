package clusterio

import (
    "context"

    . "devicedb/bucket"
    . "devicedb/data"
)

type ClusterIOAgent interface {
    Batch(ctx context.Context, siteID string, bucket string, updateBatch *UpdateBatch) (replicas int, nApplied int, err error)
    Get(ctx context.Context, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error)
    GetMatches(ctx context.Context, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error)
    CancelAll()
}

type PartitionResolver interface {
    Partition(partitioningKey string) uint64
    ReplicaNodes(partition uint64) []uint64
}

type NodeClient interface {
    Batch(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error
    Get(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error)
    GetMatches(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error)
}

type NodeReadMerger interface {
    // Add to the pool of replicas for this key
    InsertKeyReplica(nodeID uint64, key string, siblingSet *SiblingSet)
    // Get the merged set for this key
    Get(key string) *SiblingSet
    // Obtain a patch that needs to be merged into the specified node to bring it up to date
    // for any keys for which there are updates that it has not received
    Patch(nodeID uint64) map[string]*SiblingSet
}

type NodeReadRepairer interface {
    BeginRepair(readMerger NodeReadMerger)
    StopRepairs()
}