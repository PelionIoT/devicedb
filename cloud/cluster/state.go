package cluster

import (
    ddbRaft "devicedb/cloud/raft"
)

type ClusterControllerState int

const (
    StateStable ClusterControllerState = 0
)

type ClusterState struct {
    // My ID
    NodeID uint64
    // Ring members and their addresses
    PeerAddresses map[uint64]ddbRaft.PeerAddress
    // The capacity of each node in bytes
    NodeCapacities map[uint64]uint64
    // Mapping between token and node id
    Tokens map[uint64]uint64
    // Number of partitions in ring
    Partitions uint64
    // Map of which partitions are currently update-locked
    PartitionLocks map[uint64]bool
    // Ring update lock queue
    UpdateLocks []uint64
    // Replication factor of the cluster
    ReplicationFactor uint8
    // Processing state of the node
    State ConfigControllerState
}

func (clusterState *ClusterState) Snapshot() ([]byte, error) {
    return json.Marshal(clusterState)
}

func (clusterState *ClusterState) Recover(snapshot []byte) error {
    var cs ClusterState
    err := json.Unmarshal(snapshot, &cs)

    if err != nil {
        return err
    }

    *clusterState = cs

    return nil
}
