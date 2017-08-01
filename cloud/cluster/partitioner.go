package cluster

import (
    "errors"
)

var EPreconditionFailed = errors.New("Unable to validate precondition")
var ENoNodesAvailable = errors.New("Unable to assign tokens because there are no available nodes in the cluster")

type PartitioningStrategy interface {
    AssignTokens(nodes []NodeConfig, currentTokenAssignment []uint64, partitions uint64) ([]uint64, error)
}

// Simple replication strategy that does not account for capacity other than finding nodes
// that are marked as having 0 capacity to account for decomissioned nodes. Other than that
// It just tries to assign as close to an even amount of tokens to each node as possible
type SimplePartitioningStrategy struct {
}

func (ps *SimplePartitioningStrategy) countAvailableNodes(nodes []NodeConfig) int {
    availableNodes := 0

    for _, node := range nodes {
        if node.Capacity == 0 {
            continue
        }

        availableNodes++
    }

    return availableNodes
}

func (ps *SimplePartitioningStrategy) countTokens(nodes []NodeConfig) []uint64 {
    tokens := make([]uint64, len(nodes))

    for i, node := range nodes {
        tokens[i] = uint64(len(node.Tokens))
    }

    return tokens
}

func (ps *SimplePartitioningStrategy) checkPreconditions(nodes []NodeConfig, currentAssignments []uint64, partitions uint64) error {
    // Precondition 1: nodes must be non-nil
    if nodes == nil {
        return EPreconditionFailed
    }

    // Precondition 2: nodes must be sorted in order of ascending node id and all node ids are unique
    if !ps.nodesAreSortedAndUnique(nodes) {
        return EPreconditionFailed
    }

    // Precondition 3: The length of currentAssignments must be equal to partitions
    if uint64(len(currentAssignments)) != partitions {
        return EPreconditionFailed
    }

    // Precondition 4: partitions must be non-zero
    if partitions == 0 {
        return EPreconditionFailed
    }

    // Precondition 5: For all assignments in currentAssignments, the node a token is assigned to must exist in nodes[] unless it is set to zero
    // which indicates that the node does not exist
    for _, owner := range currentAssignments {
        if owner == 0 {
            continue
        }

        ownerExists := false

        for _, node := range nodes {
            if node.Address.NodeID == owner {
                ownerExists = true

                break
            }
        }

        if !ownerExists {
            return EPreconditionFailed
        }
    }

    return nil
}

func (ps *SimplePartitioningStrategy) nodesAreSortedAndUnique(nodes []NodeConfig) bool {
    var lastNodeID uint64 = 0

    for _, node := range nodes {
        if lastNodeID >= node.Address.NodeID {
            return false
        }

        lastNodeID = node.Address.NodeID
    }

    return true
}

func (ps *SimplePartitioningStrategy) AssignTokens(nodes []NodeConfig, currentAssignments []uint64, partitions uint64) ([]uint64, error) {
    if err := ps.checkPreconditions(nodes, currentAssignments, partitions); err != nil {
        return nil, err
    }

    // Precondition 6: The number of nodes must be <= partitions
    if uint64(len(nodes)) > partitions {
        // in this case return a valid assignment using just a subset of nodes. limit the number of nodes
        // that are assigned tokens to be <= partitions
        nodes = nodes[:partitions]
    }

    assignments := make([]uint64, partitions)
    tokenCounts := ps.countTokens(nodes)
    availableNodes := ps.countAvailableNodes(nodes)

    if availableNodes == 0 {
        return nil, ENoNodesAvailable
    }

    tokenCountFloor := partitions / uint64(availableNodes)
    tokenCountCeil := tokenCountFloor

    if partitions % uint64(availableNodes) != 0 {
        tokenCountCeil += 1
    }

    copy(assignments, currentAssignments)

    // unassign any token owned by a decommissioned node
    for i, node := range nodes {
        if node.Capacity != 0 {
            continue
        }

        // release tokens owned by this decommissioned node
        for token, _ := range node.Tokens {
            assignments[token] = 0
            tokenCounts[i]--
        }
    }

    // find an owner for unplaced tokens. Tokens may be unplaced due to an uninitialized cluster,
    // removed nodes, or decommissioned nodes
    for token, owner := range assignments {
        if owner != 0 {
            continue
        }

        // Token is unassigned. Need to find a home for it
        for i, node := range nodes {
            if node.Capacity == 0 {
                // This node is decommissioning. It is effectively removed from the cluster
                continue
            }

            if tokenCounts[i] < tokenCountCeil {
                assignments[token] = node.Address.NodeID
                tokenCounts[i]++

                break
            }
        }
    }

    // invariant: all tokens should be placed at some non-decomissioned node

    for i, _ := range nodes {
        if nodes[i].Capacity == 0 {
            // The ith node is decommissioning. It should receive none of the tokens
            continue
        }

        for j := 0; tokenCounts[i] < tokenCountFloor && j < len(tokenCounts); j++ {
            if j == i || tokenCounts[j] <= tokenCountFloor {
                // a node can't steal a token from itself and it can't steal a token
                // from a node that doesn't have surplus tokens
                continue
            }

            // steal a token from the jth node
            for token, owner := range assignments {
                if owner == uint64(j) {
                    assignments[token] = nodes[i].Address.NodeID
                    tokenCounts[i]++
                    tokenCounts[j]--
                    
                    break
                }
            }
        }

        // loop invariant: all nodes in nodes[:i+1] that have positive capacity have been assigned at least tokenCountFloor tokens and at most tokenCountCeil tokens
    }

    return assignments, nil
}