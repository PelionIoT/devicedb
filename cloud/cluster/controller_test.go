package cluster_test

import (
    . "devicedb/cloud/cluster"
    . "devicedb/cloud/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

type testPartitioningStrategy struct {
    calls int
    results [][]uint64
}

func (ps *testPartitioningStrategy) AssignTokens(nodes []NodeConfig, currentTokenAssignment []uint64, partitions uint64) ([]uint64, error) {
    ps.calls++

    if len(ps.results) == 0 {
        return currentTokenAssignment, nil
    }

    result := ps.results[0]
    ps.results = ps.results[1:]

    return result, nil
}

// drains the channel without expecting a set order
func expectTokenLosses(ch <-chan ClusterStateDelta, deltas map[uint64]ClusterStateDelta) {
    for len(deltas) != 0 {
        nextDelta := <-ch
        expectedDelta, ok := deltas[nextDelta.Delta.(NodeLoseToken).Token]

        Expect(ok).Should(BeTrue())
        Expect(nextDelta).Should(Equal(expectedDelta))

        delete(deltas, nextDelta.Delta.(NodeLoseToken).Token)
    }
}

// drains the channel without expecting a set order
func expectTokenGains(ch <-chan ClusterStateDelta, deltas map[uint64]ClusterStateDelta) {
    for len(deltas) != 0 {
        nextDelta := <-ch
        expectedDelta, ok := deltas[nextDelta.Delta.(NodeGainToken).Token]

        Expect(ok).Should(BeTrue())
        Expect(nextDelta).Should(Equal(expectedDelta))

        delete(deltas, nextDelta.Delta.(NodeGainToken).Token)
    }
}

var _ = Describe("Controller", func() {
    Describe("ClusterController", func() {
        Describe("#UpdateNodeConfig", func() {
            It("should update the address to whatever is specified in the command if the node exists in the cluster", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: nil,
                }

                clusterCommand := ClusterUpdateNodeBody{
                    NodeID: 1,
                    NodeConfig: NodeConfig{
                        Capacity: 1,
                        Address: PeerAddress{
                            Host: "example.com",
                            Port: 8080,
                        },
                    },
                }

                clusterController.UpdateNodeConfig(clusterCommand)

                Expect(clusterController.State.Nodes[1].Address.Host).Should(Equal("example.com"))
                Expect(clusterController.State.Nodes[1].Address.Port).Should(Equal(8080))
                Expect(clusterController.State.Nodes[2].Address.Host).Should(Equal(""))
                Expect(clusterController.State.Nodes[2].Address.Port).Should(Equal(0))
            })

            It("should update the capacity to whatever is specified in the command if the node exists in the cluster and re-distribute the tokens", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                partitioningStrategy := &testPartitioningStrategy{ }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterUpdateNodeBody{
                    NodeID: 1,
                    NodeConfig: NodeConfig{
                        Capacity: 2,
                    },
                }

                clusterController.UpdateNodeConfig(clusterCommand)

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(2)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(1))
            })

            It("should cause cause a notification of losing tokens when the capacity of the local node is updated from a positive value to zero", func() {
            })
        })

        Describe("#AddNode", func() {
            It("should add a node to the cluster", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                partitioningStrategy := &testPartitioningStrategy{ }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterAddNodeBody{
                    NodeID: 2,
                    NodeConfig: node2,
                }

                clusterController.AddNode(clusterCommand)

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(1)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(1))
            })

            It("should do nothing if the node is already part of the cluster", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                partitioningStrategy := &testPartitioningStrategy{ }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterAddNodeBody{
                    NodeID: 2,
                    NodeConfig: node2,
                }

                clusterController.AddNode(clusterCommand)

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(1)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(0))
            })

            It("should trigger a token assignment following its add notification if added node is local node", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true, 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                    Tokens: []uint64{ 1, 1, 1, 1 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 2, 2 }, // this is the new token assignment that will happen
                    },
                }
                localUpdates := make(chan ClusterStateDelta, 3) // make this a buffered node so the call to AddNode() doesn't block
                clusterController := &ClusterController{
                    LocalNodeID: 2, // set this to 2 so the added node is this node
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                    LocalUpdates: localUpdates,
                }

                clusterCommand := ClusterAddNodeBody{
                    NodeID: 2,
                    NodeConfig: node2,
                }

                clusterController.AddNode(clusterCommand)

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(1)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(1))
                Expect(<-localUpdates).Should(Equal(ClusterStateDelta{ Type: DeltaNodeAdd, Delta: NodeAdd{ NodeID: 2, NodeConfig: node2 } }))

                expectTokenGains(localUpdates, map[uint64]ClusterStateDelta{ 
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 3 } },
                })
            })

            It("should trigger a token removal if added node is not local node and added node is stealing tokens from me", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true, 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                    Tokens: []uint64{ 1, 1, 1, 1 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 2, 2 }, // this is the new token assignment that will happen
                    },
                }
                localUpdates := make(chan ClusterStateDelta, 2) // make this a buffered node so the call to AddNode() doesn't block
                clusterController := &ClusterController{
                    LocalNodeID: 1, // set this to 2 so the added node is this node
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                    LocalUpdates: localUpdates,
                }

                clusterCommand := ClusterAddNodeBody{
                    NodeID: 2,
                    NodeConfig: node2,
                }

                clusterController.AddNode(clusterCommand)

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(1)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(1))

                expectTokenLosses(localUpdates, map[uint64]ClusterStateDelta{ 
                    2: ClusterStateDelta{ Type: DeltaNodeLoseToken, Delta: NodeLoseToken{ NodeID: 1, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeLoseToken, Delta: NodeLoseToken{ NodeID: 1, Token: 3 } },
                })
            })
        })

        Describe("#RemoveNode", func() {
            It("should remove a node from a cluster", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                partitioningStrategy := &testPartitioningStrategy{ }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 2,
                }

                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                clusterController.RemoveNode(clusterCommand)
                Expect(len(clusterController.State.Nodes)).Should(Equal(1))
                Expect(partitioningStrategy.calls).Should(Equal(1))
            })

            It("should do nothing if the node isn't part of the cluster", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                }
                partitioningStrategy := &testPartitioningStrategy{ }
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 3,
                }

                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                clusterController.RemoveNode(clusterCommand)
                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                Expect(partitioningStrategy.calls).Should(Equal(0))

            })

            It("should trigger a remove notification if it is the local node being removed", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    Tokens: map[uint64]bool{ 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                    Tokens: []uint64{ 1, 1, 2, 2 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 1, 1 }, // this is the new token assignment that will happen
                    },
                }
                localUpdates := make(chan ClusterStateDelta, 3) // make this a buffered node so the call to RemoveNode() doesn't block
                clusterController := &ClusterController{
                    LocalNodeID: 2, // set this to 2 so the added node is this node
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                    LocalUpdates: localUpdates,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 2,
                }

                clusterController.RemoveNode(clusterCommand)

                Expect(partitioningStrategy.calls).Should(Equal(1))
                // Note: no token remove notifications are sent if the node is being removed, although it has lost ownership of all tokens implicitly
                Expect(<-localUpdates).Should(Equal(ClusterStateDelta{ Type: DeltaNodeRemove, Delta: NodeRemove{ NodeID: 2 } }))
            })

            It("should trigger a token gain notification it is not the local node being and the local node is gaining some of the removed nodes tokens", func() {
                node1 := NodeConfig{ 
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    Tokens: map[uint64]bool{ 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                    },
                    Partitions: [][]*PartitionReplica {
                        []*PartitionReplica{ },
                        []*PartitionReplica{ &PartitionReplica{ Partition: 1, Replica: 0, Holder: 1 } },
                    },
                    Tokens: []uint64{ 1, 1, 2, 2 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 1, 1 }, // this is the new token assignment that will happen
                    },
                }
                localUpdates := make(chan ClusterStateDelta, 3) // make this a buffered node so the call to RemoveNode() doesn't block
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                    LocalUpdates: localUpdates,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 2,
                }

                clusterController.RemoveNode(clusterCommand)

                Expect(partitioningStrategy.calls).Should(Equal(1))

                expectTokenGains(localUpdates, map[uint64]ClusterStateDelta{ 
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 3 } },
                })
            })
        })

        Describe("#TakePartitionReplica", func() {
        })

        Describe("#SetReplicationFactor", func() {
        })

        Describe("#SetPartitionCount", func() {
        })
    })
})
