package cluster

import (
    "encoding/json"
    "encoding/binary"
    "errors"
)

var ENoSuchCommand = errors.New("The cluster command type is not supported")
var ENoSuchNode = errors.New("The node specified in the update does not exist")
var ECouldNotParseCommand = errors.New("The cluster command data was not properly formatted. Unable to parse it.")

type ClusterController struct {
    LocalNodeID uint64
    State ClusterState
    ReplicationStrategy ReplicationStrategy
}

func (clusterController *ClusterController) Step(clusterCommand ClusterCommand) error {
    switch clusterCommand.Type {
    case ClusterUpdateNode:
        return clusterController.UpdateNodeConfig(clusterCommand)
    case ClusterAddNode:
        return clusterController.AddNode(clusterCommand)
    case ClusterRemoveNode:
        return clusterController.RemoveNode(clusterCommand)
    //case ClusterTakePartitionReplica:
    //    return clusterController.TakePartitionReplica(clusterCommand)
    case ClusterSetReplicationFactor:
        return clusterController.SetReplicationFactor(clusterCommand)
    case ClusterSetPartitionCount:
        return clusterController.SetPartitionCount(clusterCommand)
    default:
        return ENoSuchCommand
    }
}

func (clusterController *ClusterController) UpdateNodeConfig(clusterCommand ClusterCommand) error {
    var nodeConfig NodeConfig

    if err := json.Unmarshal(clusterCommand.Data, &nodeConfig); err != nil {
        return ECouldNotParseCommand
    }

    return clusterController.updateNodeConfig(nodeConfig)
}

func (clusterController *ClusterController) updateNodeConfig(nodeConfig NodeConfig) error {
    currentNodeConfig, ok := clusterController.State.Nodes[nodeConfig.Address.NodeID]

    if !ok {
        return ENoSuchNode
    }

    if nodeConfig.Address != currentNodeConfig.Address {
        clusterController.State.Nodes[nodeConfig.Address.NodeID].Address = nodeConfig.Address
        // TODO submit node address delta
    }

    if nodeConfig.Capacity != currentNodeConfig.Capacity {
        clusterController.State.Nodes[nodeConfig.Address.NodeID].Capacity = nodeConfig.Capacity

        // TODO submit node capacity delta

        // a capacity change with any node means tokens need to be redistributed to account for different
        // relative capacity of the nodes
        clusterController.assignTokens()
    }

    return nil
}

func (clusterController *ClusterController) AddNode(clusterCommand ClusterCommand) error {
    var nodeConfig NodeConfig

    if err := json.Unmarshal(clusterCommand.Data, &nodeConfig); err != nil {
        return ECouldNotParseCommand
    }

    if _, ok := clusterController.State.Nodes[nodeConfig.Address.NodeID]; !ok {
        // add the node if it isn't already added
        clusterController.State.AddNode(nodeConfig)

        // TODO submit node add delta

        // redistribute tokens in the cluster. tokens will be reassigned from other nodes to this node to distribute the load
        clusterController.assignTokens()
    }

    return nil
}

func (clusterController *ClusterController) RemoveNode(clusterCommand ClusterCommand) error {
    var nodeConfig NodeConfig

    if err := json.Unmarshal(clusterCommand.Data, &nodeConfig); err != nil {
        return ECouldNotParseCommand
    }

    if _, ok := clusterController.State.Nodes[nodeConfig.Address.NodeID]; ok {
        // remove the node if it isn't already removed
        clusterController.State.RemoveNode(nodeConfig.Address.NodeID)

        // TODO submit node remove delta
        
        // redistribute tokens in the cluster, making sure to distribute tokens that were owned by this node to other nodes
        clusterController.assignTokens()
    }

    return nil
}

func (clusterController *ClusterController) SetReplicationFactor(clusterCommand ClusterCommand) error {
    if clusterController.State.ClusterSettings.ReplicationFactor != 0 {
        // The replication factor has already been set and cannot be changed
        return nil
    }

    if len(clusterCommand.Data) != 8 {
        return ECouldNotParseCommand
    }

    replicationFactor := binary.BigEndian.Uint64(clusterCommand.Data)
    clusterController.State.ClusterSettings.ReplicationFactor = replicationFactor
    clusterController.initializeClusterIfReady()

    return nil
}

func (clusterController *ClusterController) SetPartitionCount(clusterCommand ClusterCommand) error {
    if clusterController.State.ClusterSettings.Partitions != 0 {
        // The partition count has already been set and cannot be changed
        return nil
    }

    if len(clusterCommand.Data) != 8 {
        return ECouldNotParseCommand
    }

    partitionCount := binary.BigEndian.Uint64(clusterCommand.Data)
    clusterController.State.ClusterSettings.Partitions = partitionCount
    clusterController.initializeClusterIfReady()

    return nil
}

func (clusterController *ClusterController) initializeClusterIfReady() {
    if !clusterController.State.ClusterSettings.AreInitialized() {
        // the cluster settings have not been finalized so the cluster cannot yet be initialized
        return
    }

    clusterController.State.Initialize()
    clusterController.assignTokens()
}

func (clusterController *ClusterController) assignTokens() {
    nodes := make([]NodeConfig, 0, len(clusterController.State.Nodes))

    for _, nodeConfig := range clusterController.State.Nodes {
        nodes = append(nodes, *nodeConfig)
    }

    newTokenAssignment, _ := clusterController.ReplicationStrategy.AssignTokens(nodes, clusterController.State.Tokens, clusterController.State.ClusterSettings.Partitions)

    // TODO perform diff between original token assignment and new token assignment to build deltas to place into update channel

    for token, owner := range newTokenAssignment {
        clusterController.State.AssignToken(owner, uint64(token))
    }
}

func (clusterController *ClusterController) Updates() <-chan NodeConfig {
    return nil
}
