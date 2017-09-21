package routes_test

import (
    "context"

    . "devicedb/bucket"
    . "devicedb/client"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/raft"
)

type MockClusterFacade struct {
    defaultAddNodeResponse error
    defaultRemoveNodeResponse error
    defaultReplaceNodeResponse error
    clusterClient *Client
    defaultDecommissionResponse error
    localNodeID uint64
    defaultPeerAddress PeerAddress
    defaultAddRelayResponse error
    defaultRemoveRelayResponse error
    defaultMoveRelayResponse error
    defaultAddSiteResponse error
    defaultRemoveSiteResponse error
    defaultBatchResponse error
    defaultLocalBatchResponse error
    defaultGetResponse []*SiblingSet
    defaultGetResponseError error
    defaultLocalGetResponse []*SiblingSet
    defaultLocalGetResponseError error
    defaultGetMatchesResponse SiblingSetIterator
    defaultGetMatchesResponseError error
    defaultLocalGetMatchesResponse SiblingSetIterator
    defaultLocalGetMatchesResponseError error
}

func (clusterFacade *MockClusterFacade) AddNode(ctx context.Context, nodeConfig NodeConfig) error {
    return clusterFacade.defaultAddNodeResponse
}

func (clusterFacade *MockClusterFacade) RemoveNode(ctx context.Context, nodeID uint64) error {
    return clusterFacade.defaultRemoveNodeResponse
}

func (clusterFacade *MockClusterFacade) ReplaceNode(ctx context.Context, nodeID uint64, replacementNodeID uint64) error {
    return clusterFacade.defaultReplaceNodeResponse
}

func (clusterFacade *MockClusterFacade) ClusterClient() *Client {
    return clusterFacade.clusterClient
}

func (clusterFacade *MockClusterFacade) Decommission() error {
    return clusterFacade.defaultDecommissionResponse
}

func (clusterFacade *MockClusterFacade) LocalNodeID() uint64 {
    return clusterFacade.localNodeID
}

func (clusterFacade *MockClusterFacade) PeerAddress(nodeID uint64) PeerAddress {
    return clusterFacade.defaultPeerAddress
}

func (clusterFacade *MockClusterFacade) AddRelay(ctx context.Context, relayID string) error {
    return clusterFacade.defaultAddRelayResponse
}

func (clusterFacade *MockClusterFacade) RemoveRelay(ctx context.Context, relayID string) error {
    return clusterFacade.defaultRemoveRelayResponse
}

func (clusterFacade *MockClusterFacade) MoveRelay(ctx context.Context, relayID string, siteID string) error {
    return clusterFacade.defaultMoveRelayResponse
}

func (clusterFacade *MockClusterFacade) AddSite(ctx context.Context, siteID string) error {
    return clusterFacade.defaultAddSiteResponse
}

func (clusterFacade *MockClusterFacade) RemoveSite(ctx context.Context, siteID string) error {
    return clusterFacade.defaultRemoveSiteResponse
}

func (clusterFacade *MockClusterFacade) Batch(siteID string, bucket string, updateBatch *UpdateBatch) error {
    return clusterFacade.defaultBatchResponse
}

func (clusterFacade *MockClusterFacade) LocalBatch(partition uint64, bucket string, updateBatch *UpdateBatch) error {
    return clusterFacade.defaultLocalBatchResponse
}

func (clusterFacade *MockClusterFacade) Get(siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
    return clusterFacade.defaultGetResponse, clusterFacade.defaultGetResponseError
}

func (clusterFacade *MockClusterFacade) LocalGet(partition uint64, bucket string, keys [][]byte) ([]*SiblingSet, error) {
    return clusterFacade.defaultLocalGetResponse, clusterFacade.defaultLocalGetResponseError
}

func (clusterFacade *MockClusterFacade) GetMatches(siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error) {
    return clusterFacade.defaultGetMatchesResponse, clusterFacade.defaultLocalGetMatchesResponseError
}

func (clusterFacade *MockClusterFacade) LocalGetMatches(partition uint64, bucket string, keys [][]byte) (SiblingSetIterator, error) {
    return clusterFacade.defaultLocalGetMatchesResponse, clusterFacade.defaultLocalGetMatchesResponseError
}