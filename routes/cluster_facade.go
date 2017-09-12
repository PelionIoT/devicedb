package routes

import (
    "context"

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
}