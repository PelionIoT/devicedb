package node_test

import (
    "context"

    . "devicedb/cluster"
    . "devicedb/raft"
)

type MockConfigController struct {
    clusterController *ClusterController
    defaultClusterCommandResponse error
    clusterCommandCB func(ctx context.Context, commandBody interface{})
}

func NewMockConfigController(clusterController *ClusterController) *MockConfigController {
    return &MockConfigController{
        clusterController: clusterController,
    }
}

func (configController *MockConfigController) AddNode(ctx context.Context, nodeConfig NodeConfig) error {
    return nil
}

func (configController *MockConfigController) ReplaceNode(ctx context.Context, replacedNodeID uint64, replacementNodeID uint64) error {
    return nil
}

func (configController *MockConfigController) RemoveNode(ctx context.Context, nodeID uint64) error {
    return nil
}

func (configController *MockConfigController) ClusterCommand(ctx context.Context, commandBody interface{}) error {
    configController.notifyClusterCommand(ctx, commandBody)
    return configController.defaultClusterCommandResponse
}

func (configController *MockConfigController) SetDefaultClusterCommandResponse(err error) {
    configController.defaultClusterCommandResponse = err
}

func (configController *MockConfigController) onClusterCommand(cb func(ctx context.Context, commandBody interface{})) {
    configController.clusterCommandCB = cb
}

func (configController *MockConfigController) notifyClusterCommand(ctx context.Context, commandBody interface{}) {
    configController.clusterCommandCB(ctx, commandBody)
}

func (configController *MockConfigController) OnLocalUpdates(cb func(deltas []ClusterStateDelta)) {
}

func (configController *MockConfigController) ClusterController() *ClusterController {
    return configController.clusterController
}

func (configController *MockConfigController) SetClusterController(clusterController *ClusterController) {
    configController.clusterController = clusterController
}

func (configController *MockConfigController) Start() error {
    return nil
}

func (configController *MockConfigController) Stop() {
}

func (configController *MockConfigController) CancelProposals() {
}

type MockClusterConfigControllerBuilder struct {
    defaultClusterConfigController ClusterConfigController
}

func NewMockClusterConfigControllerBuilder() *MockClusterConfigControllerBuilder {
    return &MockClusterConfigControllerBuilder{ }
}

func (builder *MockClusterConfigControllerBuilder) SetCreateNewCluster(b bool) ClusterConfigControllerBuilder {
    return builder
}

func (builder *MockClusterConfigControllerBuilder) SetLocalNodeAddress(peerAddress PeerAddress) ClusterConfigControllerBuilder {
    return builder
}

func (builder *MockClusterConfigControllerBuilder) SetRaftNodeStorage(raftStorage RaftNodeStorage) ClusterConfigControllerBuilder {
    return builder
}

func (builder *MockClusterConfigControllerBuilder) SetRaftNodeTransport(transport *TransportHub) ClusterConfigControllerBuilder {
    return builder
}

func (builder *MockClusterConfigControllerBuilder) Create() ClusterConfigController {
    return builder.defaultClusterConfigController
}

func (builder *MockClusterConfigControllerBuilder) SetDefaultConfigController(configController ClusterConfigController) {
    builder.defaultClusterConfigController = configController
}
