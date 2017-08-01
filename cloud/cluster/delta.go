package cluster

import (
    ddbRaft "devicedb/cloud/raft"
)

type ClusterStateDeltaType int

const (
    DeltaNodeAdd ClusterStateDeltaType = iota
    DeltaNodeRemove ClusterStateDeltaType = iota
    DeltaNodeAddress ClusterStateDeltaType = iota
    DeltaNodeCapacity ClusterStateDeltaType = iota
    DeltaNodeLoseToken ClusterStateDeltaType = iota
    DeltaNodeGainToken ClusterStateDeltaType = iota
    DeltaNodeLosePartitionReplica ClusterStateDeltaType = iota
    DeltaNodeGainPartitionReplica ClusterStateDeltaType = iota
)

type ClusterStateDelta struct {
    Type ClusterStateDeltaType
    Data []byte
}

type NodeAdd struct {
    NodeID uint64
    NodeConfig NodeConfig
}

type NodeRemove struct {
    NodeID uint64
}

type NodeAddress struct {
    NodeID uint64
    Address ddbRaft.PeerAddress
}

type NodeCapacity struct {
    NodeID uint64
    Capacity uint64
}

type NodeGainToken struct {
    NodeID uint64
    Token uint64
}

type NodeLoseToken struct {
    NodeID uint64
    Token uint64
}

type NodeGainPartitionReplica struct {
    NodeID uint64
    Partition uint64
    Replica uint64
}

type NodeLosePartitionReplica struct {
    NodeID uint64
    Partition uint64
    Replica uint64
}