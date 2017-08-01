package cluster_test

import (
    . "devicedb/cloud/cluster"
    . "devicedb/cloud/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

type testPartitioningStrategy struct {
    calls int
}

func (ps *testPartitioningStrategy) AssignTokens(nodes []NodeConfig, currentTokenAssignment []uint64, partitions uint64) ([]uint64, error) {
    ps.calls++

    return currentTokenAssignment, nil
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

            It("should produce a notification if the local node is the one being added", func() {
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
                localUpdates := make(chan ClusterStateDelta, 1) // make this a buffered node so the call to AddNode() doesn't block
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
            })

            It("should trigger a token assignment following its add notification", func() {
            })
        })

        Describe("#RemoveNode", func() {
            It("should remove a node from a cluster", func() {
            })

            It("should do nothing if the node isn't part of the cluster", func() {
            })

            It("should trigger a remove notification following its token assignments", func() {
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
