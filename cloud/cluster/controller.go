package cluster

import (
    "errors"
)

var ENoSuchCommand = errors.New("The cluster command type is not supported")
var ENoSuchNode = errors.New("The node specified in the update does not exist")
var ECouldNotParseCommand = errors.New("The cluster command data was not properly formatted. Unable to parse it.")

type ClusterController struct {
    LocalNodeID uint64
    State ClusterState
    PartitioningStrategy PartitioningStrategy
    LocalUpdates chan ClusterStateDelta
}

func (clusterController *ClusterController) Step(clusterCommand ClusterCommand) error {
    body, err := DecodeClusterCommand(clusterCommand.Type, clusterCommand.Data)

    if err != nil {
        return ECouldNotParseCommand
    }

    switch clusterCommand.Type {
    case ClusterUpdateNode:
        clusterController.UpdateNodeConfig(body.(ClusterUpdateNodeBody))
    case ClusterAddNode:
        clusterController.AddNode(body.(ClusterAddNodeBody))
    case ClusterRemoveNode:
        clusterController.RemoveNode(body.(ClusterRemoveNodeBody))
    case ClusterTakePartitionReplica:
        clusterController.TakePartitionReplica(body.(ClusterTakePartitionReplicaBody))
    case ClusterSetReplicationFactor:
        clusterController.SetReplicationFactor(body.(ClusterSetReplicationFactorBody))
    case ClusterSetPartitionCount:
        clusterController.SetPartitionCount(body.(ClusterSetPartitionCountBody))
    default:
        return ENoSuchCommand
    }

    return nil
}

func (clusterController *ClusterController) UpdateNodeConfig(clusterCommand ClusterUpdateNodeBody) {
    currentNodeConfig, ok := clusterController.State.Nodes[clusterCommand.NodeID]

    if !ok {
        // No such node
        return
    }

    currentNodeConfig.Address.Host = clusterCommand.NodeConfig.Address.Host
    currentNodeConfig.Address.Port = clusterCommand.NodeConfig.Address.Port

    if clusterCommand.NodeConfig.Capacity != currentNodeConfig.Capacity {
        currentNodeConfig.Capacity = clusterCommand.NodeConfig.Capacity

        // a capacity change with any node means tokens need to be redistributed to account for different
        // relative capacity of the nodes. This has no effect with the simple partitioning strategy unless
        // a node has been assigned capacity 0 indicating that it is leaving the cluster soon
        clusterController.assignTokens()
    }
}

func (clusterController *ClusterController) AddNode(clusterCommand ClusterAddNodeBody) {
    if _, ok := clusterController.State.Nodes[clusterCommand.NodeID]; !ok {
        // add the node if it isn't already added
        clusterController.State.AddNode(clusterCommand.NodeConfig)

        if clusterCommand.NodeID == clusterController.LocalNodeID {
            // notify the local node that it has been added to the cluster
            clusterController.notifyLocalNode(DeltaNodeAdd, NodeAdd{ NodeID: clusterController.LocalNodeID, NodeConfig: clusterCommand.NodeConfig })
        }

        // redistribute tokens in the cluster. tokens will be reassigned from other nodes to this node to distribute the load
        clusterController.assignTokens()
    }
}

func (clusterController *ClusterController) RemoveNode(clusterCommand ClusterRemoveNodeBody) {
    if _, ok := clusterController.State.Nodes[clusterCommand.NodeID]; ok {
        // remove the node if it isn't already removed
        clusterController.State.RemoveNode(clusterCommand.NodeID)

        // redistribute tokens in the cluster, making sure to distribute tokens that were owned by this node to other nodes
        clusterController.assignTokens()

        if clusterCommand.NodeID == clusterController.LocalNodeID {
            // notify the local node that it has been removed from the cluster
            clusterController.notifyLocalNode(DeltaNodeRemove, NodeRemove{ NodeID: clusterController.LocalNodeID })
        }
    }
}

func (clusterController *ClusterController) TakePartitionReplica(clusterCommand ClusterTakePartitionReplicaBody) {
    localNodePartitionReplicaSnapshot := clusterController.localNodePartitionReplicaSnapshot()

    if err := clusterController.State.AssignPartitionReplica(clusterCommand.Partition, clusterCommand.Replica, clusterCommand.NodeID); err != nil {
        // Log Error
    }

    clusterController.localDiffPartitionReplicasAndNotify(localNodePartitionReplicaSnapshot)
}

func (clusterController *ClusterController) localNodePartitionReplicaSnapshot() map[uint64]map[uint64]bool {
    nodeConfig, ok := clusterController.State.Nodes[clusterController.LocalNodeID]

    if !ok {
        return map[uint64]map[uint64]bool{ }
    }

    partitionReplicaSnapshot := make(map[uint64]map[uint64]bool, len(nodeConfig.PartitionReplicas))

    for partition, replicas := range nodeConfig.PartitionReplicas {
        partitionReplicaSnapshot[partition] = make(map[uint64]bool, len(replicas))

        for replica, _ := range replicas {
            partitionReplicaSnapshot[partition][replica] = true
        }
    }

    return partitionReplicaSnapshot
}

func (clusterController *ClusterController) localDiffPartitionReplicasAndNotify(partitionReplicaSnapshot map[uint64]map[uint64]bool) {
    nodeConfig, ok := clusterController.State.Nodes[clusterController.LocalNodeID]

    if !ok {
        return
    }

    // find out which partition replicas have been lost
    for partition, replicas := range partitionReplicaSnapshot {
        for replica, _ := range replicas {
            if _, ok := nodeConfig.PartitionReplicas[partition]; !ok {
                clusterController.notifyLocalNode(DeltaNodeLosePartitionReplica, NodeLosePartitionReplica{ NodeID: clusterController.LocalNodeID, Partition: partition, Replica: replica })

                continue
            }

            if _, ok := nodeConfig.PartitionReplicas[partition][replica]; !ok {
                clusterController.notifyLocalNode(DeltaNodeLosePartitionReplica, NodeLosePartitionReplica{ NodeID: clusterController.LocalNodeID, Partition: partition, Replica: replica })
            }
        }
    }

    // find out which partition replicas have been gained
    for partition, replicas := range nodeConfig.PartitionReplicas {
        for replica, _ := range replicas {
            if _, ok := partitionReplicaSnapshot[partition]; !ok {
                clusterController.notifyLocalNode(DeltaNodeGainPartitionReplica, NodeGainPartitionReplica{ NodeID: clusterController.LocalNodeID, Partition: partition, Replica: replica })

                continue
            }

            if _, ok := partitionReplicaSnapshot[partition][replica]; !ok {
                clusterController.notifyLocalNode(DeltaNodeGainPartitionReplica, NodeGainPartitionReplica{ NodeID: clusterController.LocalNodeID, Partition: partition, Replica: replica })
            }
        }
    }
}

func (clusterController *ClusterController) SetReplicationFactor(clusterCommand ClusterSetReplicationFactorBody) {
    if clusterController.State.ClusterSettings.ReplicationFactor != 0 {
        // The replication factor has already been set and cannot be changed
        return
    }

    clusterController.State.ClusterSettings.ReplicationFactor = clusterCommand.ReplicationFactor
    clusterController.initializeClusterIfReady()
}

func (clusterController *ClusterController) SetPartitionCount(clusterCommand ClusterSetPartitionCountBody) {
    if clusterController.State.ClusterSettings.Partitions != 0 {
        // The partition count has already been set and cannot be changed
        return
    }

    clusterController.State.ClusterSettings.Partitions = clusterCommand.Partitions
    clusterController.initializeClusterIfReady()
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

    newTokenAssignment, _ := clusterController.PartitioningStrategy.AssignTokens(nodes, clusterController.State.Tokens, clusterController.State.ClusterSettings.Partitions)

    localNodeTokenSnapshot := clusterController.localNodeTokenSnapshot()

    for token, owner := range newTokenAssignment {
        clusterController.State.AssignToken(owner, uint64(token))
    }

    // perform diff between original token assignment and new token assignment to build deltas to place into update channel
    clusterController.localDiffTokensAndNotify(localNodeTokenSnapshot)
}

func (clusterController *ClusterController) localNodeTokenSnapshot() map[uint64]bool {
    nodeConfig, ok := clusterController.State.Nodes[clusterController.LocalNodeID]

    if !ok {
        return map[uint64]bool{ }
    }

    tokenSnapshot := make(map[uint64]bool, len(nodeConfig.Tokens))

    for token, _ := range nodeConfig.Tokens {
        tokenSnapshot[token] = true
    }

    return tokenSnapshot
}

func (clusterController *ClusterController) localDiffTokensAndNotify(tokenSnapshot map[uint64]bool) {
    nodeConfig, ok := clusterController.State.Nodes[clusterController.LocalNodeID]

    if !ok {
        return
    }

    // find out which tokens have been lost
    for token, _ := range tokenSnapshot {
        if _, ok := nodeConfig.Tokens[token]; !ok {
            // this token was present in the original snapshot but is not there now
            clusterController.notifyLocalNode(DeltaNodeLoseToken, NodeLoseToken{ NodeID: clusterController.LocalNodeID, Token: token })
        }
    }

    // find out which tokens have been gained
    for token, _ := range nodeConfig.Tokens {
        if _, ok := tokenSnapshot[token]; !ok {
            // this token wasn't present in the original snapshot but is there now
            clusterController.notifyLocalNode(DeltaNodeGainToken, NodeGainToken{ NodeID: clusterController.LocalNodeID, Token: token })
        }
    }
}

// A channel that provides notifications for updates to configuration affecting the local node
// This includes gaining or losing ownership of tokens, gaining or losing ownership of partition
// replicas, becoming part of a cluster or being removed from a cluster
func (clusterController *ClusterController) notifyLocalNode(deltaType ClusterStateDeltaType, delta interface{ }) {
    if clusterController.LocalUpdates != nil {
        clusterController.LocalUpdates <- ClusterStateDelta{ Type: deltaType, Delta: delta }
    }
}
