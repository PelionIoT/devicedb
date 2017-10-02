package node

import (
    "context"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
)

type NodeClient struct {
    configController ClusterConfigController
}

func NewNodeClient(configController ClusterConfigController) *NodeClient {
    return &NodeClient{
        configController: configController,
    }
}

func (nodeClient *NodeClient) Merge(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet) error {
    return nil
}

func (nodeClient *NodeClient) Batch(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
    return nil
}

func (nodeClient *NodeClient) Get(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
    return nil, nil
}

func (nodeClient *NodeClient) GetMatches(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error) {
    return nil, nil
}