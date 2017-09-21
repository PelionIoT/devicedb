package routes

import (
    "context"

    . "devicedb/bucket"
    . "devicedb/data"
    . "devicedb/client"
    . "devicedb/cluster"
    . "devicedb/raft"
)

type ClusterFacade interface {
    AddNode(ctx context.Context, nodeConfig NodeConfig) error
    RemoveNode(ctx context.Context, nodeID uint64) error
    ReplaceNode(ctx context.Context, nodeID uint64, replacementNodeID uint64) error
    ClusterClient() *Client
    Decommission() error
    LocalNodeID() uint64
    PeerAddress(nodeID uint64) PeerAddress
    AddRelay(ctx context.Context, relayID string) error
    RemoveRelay(ctx context.Context, relayID string) error
    MoveRelay(ctx context.Context, relayID string, siteID string) error
    AddSite(ctx context.Context, siteID string) error
    RemoveSite(ctx context.Context, siteID string) error
    Batch(siteID string, bucket string, updateBatch *UpdateBatch) error
    LocalBatch(partition uint64, bucket string, updateBatch *UpdateBatch) error
    Get(siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error)
    LocalGet(partition uint64, bucket string, keys [][]byte) ([]*SiblingSet, error)
    GetMatches(siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error)
    LocalGetMatches(partition uint64, bucket string, keys [][]byte) (SiblingSetIterator, error)
}