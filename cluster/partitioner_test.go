package cluster_test

import (
    "fmt"

    . "devicedb/cluster"
    . "devicedb/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

func AssignmentIsValid(nodes []NodeConfig, partitions uint64, assignment []uint64) bool {
    if uint64(len(assignment)) != partitions {
        return false
    }

    if uint64(len(nodes)) > partitions {
        nodes = nodes[:partitions]
    }
    
    availableNodes := 0

    for _, node := range nodes {
        if node.Capacity != 0 {
            availableNodes++
        }
    }

    tokenCountFloor := partitions / uint64(availableNodes)
    tokenCountCeil := tokenCountFloor

    if partitions % uint64(availableNodes) != 0 {
        tokenCountCeil += 1
    }

    // At least one token assignment per node
    for _, node := range nodes {
        if node.Capacity == 0 {
            continue
        }

        ownsNode := false

        for _, owner := range assignment {
            if owner == node.Address.NodeID {
                ownsNode = true

                break
            }
        }

        if !ownsNode {
            return false
        }
    }

    // Exactly one node assigned to each token
    for _, owner := range assignment {
        if owner == 0 {
            return false
        }
    }

    // Each node owns between tokenCountFloor and tokenCountCeil tokens
    for _, node := range nodes {
        if node.Capacity == 0 {
            continue
        }

        tokenCount := 0

        for _, owner := range assignment {
            if owner == node.Address.NodeID {
                tokenCount += 1
            }
        }

        if uint64(tokenCount) > tokenCountCeil || uint64(tokenCount) < tokenCountFloor {
            return false
        }
    }

    return true
}

var _ = Describe("Partitioner", func() {
    Describe("SimplePartitioningStrategy", func() {
        It("should return EPreconditionFailed if node list is nil", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens(nil, make([]uint64, 8), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return ENoNodesAvailable if node list is empty", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ }, make([]uint64, 8), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(ENoNodesAvailable))
        })

        It("should return ENoNodesAvailable if node list has nodes but they all have 0 capacity", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ NodeConfig{ Address: PeerAddress{ NodeID: 1 } }, NodeConfig{ Address: PeerAddress{ NodeID: 2 } } }, make([]uint64, 8), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(ENoNodesAvailable))
        })

        It("should return EPreconditionFailed if node list has nodes with duplicate IDs", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ NodeConfig{ Address: PeerAddress{ NodeID: 1 } }, NodeConfig{ Address: PeerAddress{ NodeID: 1 } } }, make([]uint64, 8), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return EPreconditionFailed if node list is not sorted in order of increasing node ID", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ NodeConfig{ Address: PeerAddress{ NodeID: 2 } }, NodeConfig{ Address: PeerAddress{ NodeID: 1 } } }, make([]uint64, 8), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return EPreconditionFailed if the length of the currentAssignments array is not equal to the number of partitions", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 1 } }, NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 2 } } }, make([]uint64, 7), 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return EPreconditionFailed if the number of partitions is set to 0", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 1 } }, NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 2 } } }, make([]uint64, 0), 0)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return EPreconditionFailed if there is a non-zero node ID contained in the currentAssignments array that does not match up with a node contained in the nodes list", func() {
            ps := &SimplePartitioningStrategy{ }

            assignment, err := ps.AssignTokens([]NodeConfig{ 
                NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 1 } }, 
                NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: 2 } },
            }, []uint64{ 0, 0, 6, 0, 0, 0, 0, 0 }, 8)

            Expect(assignment).Should(BeNil())
            Expect(err).Should(Equal(EPreconditionFailed))
        })

        It("should return a valid assignment utilizing all the nodes if the number of nodes <= the number of partitions when starting from all unassigned nodes", func() {
            ps := &SimplePartitioningStrategy{ }
            var partitions uint64 = 256

            for numNodes := 1; uint64(numNodes) <= partitions; numNodes++ {
                nodes := make([]NodeConfig, numNodes)
                currentAssignment := make([]uint64, partitions)

                for i, _ := range nodes {
                    nodes[i] = NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: uint64(i) + 1 } }
                }

                assignment, err := ps.AssignTokens(nodes, currentAssignment, partitions)

                Expect(AssignmentIsValid(nodes, partitions, assignment)).Should(BeTrue())
                Expect(err).Should(BeNil())
            }
        })

        It("should return a valid assignment after a node is added", func() {
            ps := &SimplePartitioningStrategy{ }
            var partitions uint64 = 256

            nodes := make([]NodeConfig, partitions / 2)
            currentAssignment := make([]uint64, partitions)

            for i, _ := range nodes {
                nodes[i] = NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: uint64(i) + 1 } }
            }

            assignment, err := ps.AssignTokens(nodes, currentAssignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, assignment)).Should(BeTrue())
            Expect(err).Should(BeNil())

            for i, node := range nodes {
                tokens := make(map[uint64]bool)

                for token, owner := range assignment {
                    if owner == nodes[i].Address.NodeID {
                        tokens[uint64(token)] = true
                    }
                }

                nodes[i] = NodeConfig{
                    Capacity: 1,
                    Tokens: tokens,
                    Address: node.Address,
                }
            }

            nodes = append(nodes, NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: (partitions / 2) + 1 } })
            nodes = append(nodes, NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: (partitions / 2) + 2 } })
            nodes = append(nodes, NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: (partitions / 2) + 3 } })

            newAssignment, err := ps.AssignTokens(nodes, assignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, newAssignment)).Should(BeTrue())
            Expect(err).Should(BeNil())
        })

        It("should return a valid assignment after a node is removed", func() {
            ps := &SimplePartitioningStrategy{ }
            var partitions uint64 = 256

            nodes := make([]NodeConfig, partitions / 2)
            currentAssignment := make([]uint64, partitions)

            for i, _ := range nodes {
                nodes[i] = NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: uint64(i) + 1 } }
            }

            assignment, err := ps.AssignTokens(nodes, currentAssignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, assignment)).Should(BeTrue())
            Expect(err).Should(BeNil())

            for i, node := range nodes {
                tokens := make(map[uint64]bool)

                for token, owner := range assignment {
                    if owner == nodes[i].Address.NodeID {
                        tokens[uint64(token)] = true
                    }
                }

                nodes[i] = NodeConfig{
                    Capacity: 1,
                    Tokens: tokens,
                    Address: node.Address,
                }
            }

            for token, _ := range nodes[0].Tokens {
                assignment[token] = 0
            }

            nodes = nodes[1:]

            newAssignment, err := ps.AssignTokens(nodes, assignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, newAssignment)).Should(BeTrue())
            Expect(err).Should(BeNil())
        })
        
        It("should not change a valid assignment if nothing has changed", func() {
            ps := &SimplePartitioningStrategy{ }
            var partitions uint64 = 256

            nodes := make([]NodeConfig, partitions / 2)
            currentAssignment := make([]uint64, partitions)

            for i, _ := range nodes {
                nodes[i] = NodeConfig{ Capacity: 1, Address: PeerAddress{ NodeID: uint64(i) + 1 } }
            }

            assignment, err := ps.AssignTokens(nodes, currentAssignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, assignment)).Should(BeTrue())
            Expect(err).Should(BeNil())

            for i, node := range nodes {
                tokens := make(map[uint64]bool)

                for token, owner := range assignment {
                    if owner == nodes[i].Address.NodeID {
                        tokens[uint64(token)] = true
                    }
                }

                nodes[i] = NodeConfig{
                    Capacity: 1,
                    Tokens: tokens,
                    Address: node.Address,
                }
            }

            newAssignment, err := ps.AssignTokens(nodes, assignment, partitions)

            Expect(AssignmentIsValid(nodes, partitions, newAssignment)).Should(BeTrue())
            Expect(newAssignment).Should(Equal(assignment))
            Expect(err).Should(BeNil())
        })
    })

    Describe("#Owners", func() {
        It("should return an empty array if tokenAssignments is nil", func() {
            ps := &SimplePartitioningStrategy{ }
            Expect(ps.Owners(nil, 0, 3)).Should(Equal([]uint64{}))
        })

        It("should return an empty array if partition is not within the token assignments array", func() {
            ps := &SimplePartitioningStrategy{ }
            Expect(ps.Owners([]uint64{ 1, 1, 1 }, 5, 3)).Should(Equal([]uint64{}))
        })

        Context("the number of distinct nodes < the replication factor", func() {
            It("should return an array that repeats some nodes", func() {
                ps := &SimplePartitioningStrategy{ }
                Expect(ps.Owners([]uint64{ 1, 1, 1, 1, 2, 2, 2, 2 }, 0, 3)).Should(Equal([]uint64{ 1, 2, 1 }))
                Expect(ps.Owners([]uint64{ 1, 1, 1, 1, 2, 2, 2, 2 }, 4, 3)).Should(Equal([]uint64{ 2, 1, 2 }))
                Expect(ps.Owners([]uint64{ 1, 1, 1, 1, 2, 2, 2, 2 }, 7, 3)).Should(Equal([]uint64{ 2, 1, 2 }))
            })

            It("should exclude 0 as a possible node", func() {
                ps := &SimplePartitioningStrategy{ }
                Expect(ps.Owners([]uint64{ 0, 0, 0, 1, 2, 2, 2, 2 }, 0, 3)).Should(Equal([]uint64{ 1, 2, 1 }))
                Expect(ps.Owners([]uint64{ 0, 0, 0, 1, 2, 2, 2, 2 }, 4, 3)).Should(Equal([]uint64{ 2, 1, 2 }))
                Expect(ps.Owners([]uint64{ 0, 0, 0, 1, 2, 2, 2, 2 }, 7, 3)).Should(Equal([]uint64{ 2, 1, 2 }))
                Expect(ps.Owners([]uint64{ 0, 0, 0, 0, 0, 0, 0, 0 }, 7, 3)).Should(Equal([]uint64{ }))
            })
        })

        Context("the number of distinct nodes >= the replication factor", func() {
           It("should return an array that contains only the first R distinct nodes encountered while traversing the ring in a clockwise direction where R is the replication factor", func() {
                ps := &SimplePartitioningStrategy{ }
                Expect(ps.Owners([]uint64{ 1, 2, 3, 4, 5, 1, 2, 3 }, 0, 3)).Should(Equal([]uint64{ 1, 2, 3 }))
                Expect(ps.Owners([]uint64{ 1, 2, 3, 4, 5, 1, 2, 3 }, 4, 3)).Should(Equal([]uint64{ 5, 1, 2 }))
                Expect(ps.Owners([]uint64{ 1, 2, 3, 4, 5, 1, 2, 3 }, 7, 3)).Should(Equal([]uint64{ 3, 1, 2 }))
                Expect(ps.Owners([]uint64{ 1, 2, 1, 4, 5, 1, 2, 1 }, 7, 3)).Should(Equal([]uint64{ 1, 2, 4 }))
            })
        })
    })

    Describe("#Partition", func() {
        Specify("Should return a bunch of partition numbers that are < partition count", func() {
            ps := &SimplePartitioningStrategy{ }
            buckets := map[uint64]int{ }

            for i := 0; i < 10000; i++ {
                key := fmt.Sprintf("key-%d", i)
                Expect(ps.Partition(key, 64)).Should(Equal(ps.Partition(key, 64)))
                Expect(ps.Partition(key, 64) < 64).Should(BeTrue())
                buckets[ps.Partition(key, 64)] = buckets[ps.Partition(key, 64)] + 1
            }

            for i := 0; i < 64; i += 1 {
                count, ok := buckets[uint64(i)]

                Expect(ok).Should(BeTrue())
                Expect(count > 0).Should(BeTrue())
            }
        })
    })

    Describe("#CalculateShiftAmount", func() {
        Specify("Should set shift amount to (64 - P) where 2^P = the partition count", func() {
            for i := uint(0); i < 64; i++ {
                ps := &SimplePartitioningStrategy{ }
                Expect(ps.CalculateShiftAmount(1 << i)).Should(Equal(int(64 - i)))
            }
        })
    })
})
