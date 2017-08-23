package cluster_test

import (
    . "devicedb/cluster"
    . "devicedb/raft"

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
func expectTokenLosses(actualDeltas []ClusterStateDelta, deltas map[uint64]ClusterStateDelta) {
    for len(deltas) != 0 {
        nextDelta := actualDeltas[0]
        actualDeltas = actualDeltas[1:]
        expectedDelta, ok := deltas[nextDelta.Delta.(NodeLoseToken).Token]

        Expect(ok).Should(BeTrue())
        Expect(nextDelta).Should(Equal(expectedDelta))

        delete(deltas, nextDelta.Delta.(NodeLoseToken).Token)
    }
}

// drains the channel without expecting a set order
func expectTokenGains(actualDeltas []ClusterStateDelta, deltas map[uint64]ClusterStateDelta) {
    for len(deltas) != 0 {
        nextDelta := actualDeltas[0]
        actualDeltas = actualDeltas[1:]
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 2 },
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 2 },
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                clusterController := &ClusterController{
                    LocalNodeID: 2, // set this to 2 so the added node is this node
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterAddNodeBody{
                    NodeID: 2,
                    NodeConfig: node2,
                }

                clusterController.AddNode(clusterCommand)
                node2.Tokens = map[uint64]bool{ 2: true, 3: true }

                Expect(clusterController.State.Nodes[1].Capacity).Should(Equal(uint64(1)))
                Expect(clusterController.State.Nodes[2].Capacity).Should(Equal(uint64(1)))
                Expect(partitioningStrategy.calls).Should(Equal(1))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeAdd, Delta: NodeAdd{ NodeID: 2, NodeConfig: node2 } }))

                expectTokenGains(clusterController.Deltas()[1:], map[uint64]ClusterStateDelta{
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                clusterController := &ClusterController{
                    LocalNodeID: 1, // set this to 2 so the added node is this node
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

                expectTokenLosses(clusterController.Deltas(), map[uint64]ClusterStateDelta{
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
            
            It("should do nothing if the specified replacement node does not exist", func() {
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                    ReplacementNodeID: 3,
                }

                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                clusterController.RemoveNode(clusterCommand)
                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                Expect(partitioningStrategy.calls).Should(Equal(0))
            })

            It("should do nothing if the specified replacement node exists but has already been allocated tokens", func() {
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
                node3 := NodeConfig{
                    Capacity: 0,
                    Address: PeerAddress{ NodeID: 3 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                    Tokens: map[uint64]bool{ 1: true },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                        3: &node3,
                    },
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                    ReplacementNodeID: 3,
                }

                Expect(len(clusterController.State.Nodes)).Should(Equal(3))
                clusterController.RemoveNode(clusterCommand)
                Expect(len(clusterController.State.Nodes)).Should(Equal(3))
                Expect(partitioningStrategy.calls).Should(Equal(0))
            })

            It("should hand the tokens of the removed node to the specified replacement node if it exists and give it its capacity", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                    Tokens: map[uint64]bool{ 1: true, 2: true },
                }
                node3 := NodeConfig{
                    Capacity: 0,
                    Address: PeerAddress{ NodeID: 3 },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                    Tokens: map[uint64]bool{ },
                }
                clusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{ 
                        1: &node1,
                        2: &node2,
                        3: &node3,
                    },
                    Tokens: []uint64{ 0, 2, 2 },
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                    ReplacementNodeID: 3,
                }

                Expect(len(clusterController.State.Nodes)).Should(Equal(3))
                clusterController.RemoveNode(clusterCommand)
                Expect(len(clusterController.State.Nodes)).Should(Equal(2))
                Expect(partitioningStrategy.calls).Should(Equal(0))
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                clusterController := &ClusterController{
                    LocalNodeID: 2, // set this to 2 so the added node is this node
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 2,
                }

                clusterController.RemoveNode(clusterCommand)

                Expect(partitioningStrategy.calls).Should(Equal(1))
                // Note: no token remove notifications are sent if the node is being removed, although it has lost ownership of all tokens implicitly
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeRemove, Delta: NodeRemove{ NodeID: 2 } }))
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
                    ClusterSettings: ClusterSettings{ Partitions: 4, ReplicationFactor: 1 },
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
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                clusterCommand := ClusterRemoveNodeBody{
                    NodeID: 2,
                }

                clusterController.RemoveNode(clusterCommand)

                Expect(partitioningStrategy.calls).Should(Equal(1))

                expectTokenGains(clusterController.Deltas(), map[uint64]ClusterStateDelta{ 
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 3 } },
                })
            })
        })

        Describe("#TakePartitionReplica", func() {
            It("should assign a partition replica to a node if they are all valid", func() {
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
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                }

                // moves partition 1 replica 0 from node 1 to node 2
                clusterCommand := ClusterTakePartitionReplicaBody{
                    Partition: 1,
                    Replica: 0,
                    NodeID: 2,
                }

                clusterController.TakePartitionReplica(clusterCommand)
                Expect(clusterController.State.Nodes[1].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ }))
                Expect(clusterController.State.Nodes[2].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ 1: { 0: true } }))
            })

            It("should provide a notification that the local node has lost a partition replica if another node takes it", func() {
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
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: clusterState,
                }

                // moves partition 1 replica 0 from node 1 to node 2
                clusterCommand := ClusterTakePartitionReplicaBody{
                    Partition: 1,
                    Replica: 0,
                    NodeID: 2,
                }

                clusterController.TakePartitionReplica(clusterCommand)
                Expect(clusterController.State.Nodes[1].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ }))
                Expect(clusterController.State.Nodes[2].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ 1: { 0: true } }))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeLosePartitionReplica, Delta: NodeLosePartitionReplica{ NodeID: 1, Partition: 1, Replica: 0 } }))
            })

            It("should provide a notification that the local node has gained a partition replica if it is the one taking it", func() {
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
                clusterController := &ClusterController{
                    LocalNodeID: 2,
                    State: clusterState,
                }

                // moves partition 1 replica 0 from node 1 to node 2
                clusterCommand := ClusterTakePartitionReplicaBody{
                    Partition: 1,
                    Replica: 0,
                    NodeID: 2,
                }

                clusterController.TakePartitionReplica(clusterCommand)
                Expect(clusterController.State.Nodes[1].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ }))
                Expect(clusterController.State.Nodes[2].PartitionReplicas).Should(Equal(map[uint64]map[uint64]bool{ 1: { 0: true } }))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeGainPartitionReplica, Delta: NodeGainPartitionReplica{ NodeID: 2, Partition: 1, Replica: 0 } }))
            })
        })

        Describe("#SetReplicationFactor", func() {
            It("should set the replication factor only if it has not yet been set", func() {
                clusterState := ClusterState{ }
                clusterController := &ClusterController{ State: clusterState }

                Expect(clusterController.State.ClusterSettings.ReplicationFactor).Should(Equal(uint64(0)))
                clusterController.SetReplicationFactor(ClusterSetReplicationFactorBody{ ReplicationFactor: 4 })
                Expect(clusterController.State.ClusterSettings.ReplicationFactor).Should(Equal(uint64(4)))
                clusterController.SetReplicationFactor(ClusterSetReplicationFactorBody{ ReplicationFactor: 5 })
                Expect(clusterController.State.ClusterSettings.ReplicationFactor).Should(Equal(uint64(4)))
            })

            It("should create a token assignment and notify the local node of its tokens upon triggering an initialization", func() {
               node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
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
                        2: &node2,
                    },
                    ClusterSettings: ClusterSettings{ Partitions: 4 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 2, 2 }, // this is the new token assignment that will happen
                    },
                }
                clusterController := &ClusterController{
                    LocalNodeID: 2,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                // moves partition 1 replica 0 from node 1 to node 2
                clusterCommand := ClusterSetReplicationFactorBody{
                    ReplicationFactor: 3,
                }

                clusterController.SetReplicationFactor(clusterCommand)
                expectTokenGains(clusterController.Deltas(), map[uint64]ClusterStateDelta{
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 3 } },
                })
            })
        })

        Describe("#SetPartitionCount", func() {
            It("should set the partition count only if it has not yet been set", func() {
                clusterState := ClusterState{ }
                clusterController := &ClusterController{ State: clusterState }

                Expect(clusterController.State.ClusterSettings.Partitions).Should(Equal(uint64(0)))
                clusterController.SetPartitionCount(ClusterSetPartitionCountBody{ Partitions: 8 })
                Expect(clusterController.State.ClusterSettings.Partitions).Should(Equal(uint64(8)))
                clusterController.SetPartitionCount(ClusterSetPartitionCountBody{ Partitions: 10 })
                Expect(clusterController.State.ClusterSettings.Partitions).Should(Equal(uint64(8)))
            })

            It("should create a token assignment and notify the local node of its tokens upon triggering an initialization", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
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
                        2: &node2,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2 },
                }
                partitioningStrategy := &testPartitioningStrategy{ 
                    results: [][]uint64{
                        []uint64{ 1, 1, 2, 2 }, // this is the new token assignment that will happen
                    },
                }
                clusterController := &ClusterController{
                    LocalNodeID: 2,
                    State: clusterState,
                    PartitioningStrategy: partitioningStrategy,
                }

                // moves partition 1 replica 0 from node 1 to node 2
                clusterCommand := ClusterSetPartitionCountBody{
                    Partitions: 8,
                }

                clusterController.SetPartitionCount(clusterCommand)
                expectTokenGains(clusterController.Deltas(), map[uint64]ClusterStateDelta{
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 2, Token: 3 } },
                })
            })
        })
        
        Describe("#ApplySnapshot", func() {
            It("should restore cluster state to the state encoded in the snapshot", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                node2 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 2 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        2: &node2,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 3, Partitions: 9 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in

                clusterController := &ClusterController{
                    LocalNodeID: 2,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
            })

            It("should notify the node that it has been removed from the cluster if the first snapshot has the node in it and the next one doesnt", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeRemove, Delta: NodeRemove{ NodeID: 1 } }))
            })

            It("should notify the node that it has been added to the cluster if the first snapshot doesnt have the node in it and the next one does", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeAdd, Delta: NodeAdd{ NodeID: 1, NodeConfig: node1 } }))
            })

            It("should notify the node of tokens that it has gained ownership of if the node does not originally own it but the snapshot gives it ownership", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                node1Snap := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true, 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1Snap,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Tokens = []uint64{ 1, 1, 2, 2 }
                snapshotClusterState.Tokens = []uint64{ 1, 1, 1, 1 }

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                expectTokenGains(clusterController.Deltas(), map[uint64]ClusterStateDelta{
                    2: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeGainToken, Delta: NodeGainToken{ NodeID: 1, Token: 3 } },
                })
            })

            It("should notify the node of tokens that it has lost ownership of if the node originally owns it but the snapshot takes its ownership away", func() {
                node1 := NodeConfig{
                    Capacity: 1,
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true, 2: true, 3: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                node1Snap := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ 0: true, 1: true },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1Snap,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Tokens = []uint64{ 1, 1, 1, 1 }
                snapshotClusterState.Tokens = []uint64{ 1, 1, 2, 2 }

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                expectTokenLosses(clusterController.Deltas(), map[uint64]ClusterStateDelta{
                    2: ClusterStateDelta{ Type: DeltaNodeLoseToken, Delta: NodeLoseToken{ NodeID: 1, Token: 2 } },
                    3: ClusterStateDelta{ Type: DeltaNodeLoseToken, Delta: NodeLoseToken{ NodeID: 1, Token: 3 } },
                })
            })

            It("should notify the node of partition replicas that it has gained ownership of if the node does not originally own it but the snapshot gives it ownership", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                node1Snap := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1Snap,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeGainPartitionReplica, Delta: NodeGainPartitionReplica{ NodeID: 1, Partition: 1, Replica: 0 } }))
            })

            It("should notify the node of partition replicas that it has lost ownership of if the node originally owns it but the snapshot takes its ownership away", func() {
                node1 := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ 1: { 0: true } },
                }
                node1Snap := NodeConfig{
                    Capacity: 1, 
                    Address: PeerAddress{ NodeID: 1 },
                    Tokens: map[uint64]bool{ },
                    PartitionReplicas: map[uint64]map[uint64]bool{ },
                }
                originalClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 },
                }
                snapshotClusterState := ClusterState{
                    Nodes: map[uint64]*NodeConfig{
                        1: &node1Snap,
                    },
                    ClusterSettings: ClusterSettings{ ReplicationFactor: 2, Partitions: 4 }, // normally these values dont change but to test the difference we will change these
                }
                snapshotClusterState.Initialize() // makes sure tokens and partition replicas are filled in
                originalClusterState.Initialize() // makes sure tokens and partition replicas are filled in

                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: originalClusterState,
                }

                snap, _ := snapshotClusterState.Snapshot()

                Expect(clusterController.State).Should(Equal(originalClusterState))
                Expect(clusterController.ApplySnapshot(snap)).Should(BeNil())
                Expect(clusterController.State).Should(Equal(snapshotClusterState))
                Expect(clusterController.Deltas()[0]).Should(Equal(ClusterStateDelta{ Type: DeltaNodeLosePartitionReplica, Delta: NodeLosePartitionReplica{ NodeID: 1, Partition: 1, Replica: 0 } }))
            })
        })
    })
})
