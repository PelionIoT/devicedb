package cloud

import (
    "encoding/json"
)

type NodeState struct {
    // My ID
    NodeID string
    // Ring members and their addresses
    Nodes map[string]string
    // Mapping between node and tokens
    Tokens map[string]string
    // Number of partitions in ring
    Partitions uint32
    // Map of which partitions are currently update-locked
    PartitionLocks map[uint32]bool
    // Ring update lock queue
    Locks []string
    // Processing state of the node
    State int
}

func (nodeState *NodeState) snapshot() ([]byte, error) {
    return json.Marshal(nodeState)
}

func (nodeState *NodeState) recover(snapshot []byte) error {
    var ns NodeState
    err := json.Unmarshal(snapshot, &ns)

    if err != nil {
        return err
    }

    *nodeState = ns

    return nil
}

