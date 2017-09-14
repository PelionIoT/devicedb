package node_test

import (
    . "devicedb/cluster"
    . "devicedb/node"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

type PartitionReplicaSet struct {
    set map[uint64]map[uint64]bool
}

func NewPartitionReplicaSet() *PartitionReplicaSet {
    return &PartitionReplicaSet{
        set: make(map[uint64]map[uint64]bool, 0),
    }
}

func (partitionReplicaSet *PartitionReplicaSet) Add(partition, replica uint64) {
    if _, ok := partitionReplicaSet.set[partition]; !ok {
        partitionReplicaSet.set[partition] = make(map[uint64]bool, 0)
    }

    partitionReplicaSet.set[partition][replica] = true
}

func (partitionReplicaSet *PartitionReplicaSet) Remove(partition, replica uint64) {
    if partitionReplicaSet.set[partition] != nil {
        delete(partitionReplicaSet.set[partition], replica)
    }

    if len(partitionReplicaSet.set[partition]) == 0 {
        delete(partitionReplicaSet.set, partition)
    }
}

func (partitionReplicaSet *PartitionReplicaSet) Partitions() []uint64 {
    partitions := make([]uint64, 0, len(partitionReplicaSet.set))

    for partition, _ := range partitionReplicaSet.set {
        partitions = append(partitions, partition)
    }

    return partitions
}

func (partitionReplicaSet *PartitionReplicaSet) Replicas(partition uint64) []uint64 {
    replicas := make([]uint64, 0, len(partitionReplicaSet.set[partition]))

    for replica, _ := range partitionReplicaSet.set[partition] {
        replicas = append(replicas, replica)
    }

    return replicas
}

func (partitionReplicaSet *PartitionReplicaSet) Map() map[uint64]map[uint64]bool {
    return partitionReplicaSet.set
}

type MockNodeCoordinatorFacade struct {
    ownedPartitionReplicas *PartitionReplicaSet
    heldPartitionReplicas *PartitionReplicaSet
    partitions map[uint64]bool
    outgoingTransfersEnabled map[uint64]bool
    incomingTransfers *PartitionReplicaSet
    readLocks map[uint64]bool
    writeLocks map[uint64]bool
    joinedCluster chan int
    leftCluster chan int
    id uint64
}

func NewMockNodeCoordinatorFacade(id uint64) *MockNodeCoordinatorFacade {
    return &MockNodeCoordinatorFacade{
        id: id,
        ownedPartitionReplicas: NewPartitionReplicaSet(),
        heldPartitionReplicas: NewPartitionReplicaSet(),
        partitions: make(map[uint64]bool, 0),
        outgoingTransfersEnabled: make(map[uint64]bool, 0),
        incomingTransfers: NewPartitionReplicaSet(),
        readLocks: make(map[uint64]bool, 0),
        writeLocks: make(map[uint64]bool, 0),
        joinedCluster: make(chan int, 1),
        leftCluster: make(chan int, 1),
    }
}

func (nodeFacade *MockNodeCoordinatorFacade) ID() uint64 {
    return nodeFacade.id
}

func (nodeFacade *MockNodeCoordinatorFacade) AddPartition(partitionNumber uint64) {
    nodeFacade.partitions[partitionNumber] = true
    nodeFacade.LockPartitionReads(partitionNumber)
    nodeFacade.LockPartitionWrites(partitionNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) RemovePartition(partitionNumber uint64) {
    delete(nodeFacade.partitions, partitionNumber)
}

// which partitions have been added that have not yet been removed
func (nodeFacade *MockNodeCoordinatorFacade) Partitions() map[uint64]bool {
    return nodeFacade.partitions
}

func (nodeFacade *MockNodeCoordinatorFacade) EnableOutgoingTransfers(partitionNumber uint64) {
    nodeFacade.outgoingTransfersEnabled[partitionNumber] = true
}

func (nodeFacade *MockNodeCoordinatorFacade) DisableOutgoingTransfers(partitionNumber uint64) {
    delete(nodeFacade.outgoingTransfersEnabled, partitionNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) EnabledOutgoingTransfers() map[uint64]bool {
    return nodeFacade.outgoingTransfersEnabled
}

func (nodeFacade *MockNodeCoordinatorFacade) StartIncomingTransfer(partitionNumber uint64, replicaNumber uint64) {
    nodeFacade.incomingTransfers.Add(partitionNumber, replicaNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) StopIncomingTransfer(partitionNumber uint64, replicaNumber uint64) {
    nodeFacade.incomingTransfers.Remove(partitionNumber, replicaNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) IncomingTransfers() *PartitionReplicaSet {
    return nodeFacade.incomingTransfers
}

func (nodeFacade *MockNodeCoordinatorFacade) LockPartitionWrites(partitionNumber uint64) {
    nodeFacade.writeLocks[partitionNumber] = true
}

func (nodeFacade *MockNodeCoordinatorFacade) UnlockPartitionWrites(partitionNumber uint64) {
    delete(nodeFacade.writeLocks, partitionNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) WriteLockedPartitions() map[uint64]bool {
    return nodeFacade.writeLocks
}

func (nodeFacade *MockNodeCoordinatorFacade) LockPartitionReads(partitionNumber uint64) {
    nodeFacade.readLocks[partitionNumber] = true
}

func (nodeFacade *MockNodeCoordinatorFacade) UnlockPartitionReads(partitionNumber uint64) {
    delete(nodeFacade.readLocks, partitionNumber)
}

func (nodeFacade *MockNodeCoordinatorFacade) ReadLockedPartitions() map[uint64]bool {
    return nodeFacade.readLocks
}

func (nodeFacade *MockNodeCoordinatorFacade) OwnedPartitionReplicas() map[uint64]map[uint64]bool {
    return nodeFacade.ownedPartitionReplicas.Map()
}

func (nodeFacade *MockNodeCoordinatorFacade) OwnedPartitionReplicaSet() *PartitionReplicaSet {
    return nodeFacade.ownedPartitionReplicas
}

func (nodeFacade *MockNodeCoordinatorFacade) HeldPartitionReplicas() map[uint64]map[uint64]bool {
    return nodeFacade.heldPartitionReplicas.Map()
}

func (nodeFacade *MockNodeCoordinatorFacade) HeldPartitionReplicaSet() *PartitionReplicaSet {
    return nodeFacade.heldPartitionReplicas
}

func (nodeFacade *MockNodeCoordinatorFacade) NotifyJoinedCluster() {
    nodeFacade.joinedCluster <- 1
}

func (nodeFacade *MockNodeCoordinatorFacade) JoinedCluster() <-chan int {
    return nodeFacade.joinedCluster
}

func (nodeFacade *MockNodeCoordinatorFacade) NotifyLeftCluster() {
    nodeFacade.leftCluster <- 1
}

func (nodeFacade *MockNodeCoordinatorFacade) LeftCluster() <-chan int {
    return nodeFacade.leftCluster
}

type MockClusterNodePartitionUpdater struct {
    updatePartitionCalls map[uint64]int
}

func NewMockClusterNodePartitionUpdater() *MockClusterNodePartitionUpdater {
    return &MockClusterNodePartitionUpdater{
        updatePartitionCalls: make(map[uint64]int, 0),
    }
}

func (partitionUpdater *MockClusterNodePartitionUpdater) UpdatePartition(partitionNumber uint64) {
    partitionUpdater.updatePartitionCalls[partitionNumber] = partitionUpdater.updatePartitionCalls[partitionNumber] + 1
}

func (partitionUpdater *MockClusterNodePartitionUpdater) UpdatePartitionCalls(partitionNumber uint64) int {
    return partitionUpdater.updatePartitionCalls[partitionNumber]
}

var _ = Describe("NodeStateCoordinator", func() {
    Describe("NodePartitionUpdater", func() {
        // Degrees of freedom
        //   Variables affecting control flow
        //     . v1 (bool) At least one replica of partition is held by node
        //     . v2 (bool) At least one replica of partition is owned by node
        //   Possible affected output states
        //     . Partition instance exists in node's partition pool
        //     . Partition write lock
        //     . Partition read lock
        //     . Partition outgoing transfers enabled
        Describe("#UpdatePartition", func() {
            var nodePartitionUpdater *NodePartitionUpdater
            var nodeFacade *MockNodeCoordinatorFacade

            BeforeEach(func() {
                nodeFacade = NewMockNodeCoordinatorFacade(1)
                nodePartitionUpdater = NewNodePartitionUpdater(nodeFacade)
            })

            Context("When a node owns a replica of a partition", func() {
                BeforeEach(func() {
                    nodeFacade.OwnedPartitionReplicaSet().Add(1, 0)
                })

                Context("And the node holds a replica of that partition", func() {
                    BeforeEach(func() {
                        nodeFacade.HeldPartitionReplicaSet().Add(1, 1)
                    })

                    It("Should ensure that partition has been added to the partition pool", func() {
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ }))
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ 1: true }))
                    })

                    It("Should read unlock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        nodeFacade.LockPartitionReads(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeFalse())
                    })

                    It("Should write unlock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        nodeFacade.LockPartitionWrites(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeFalse())
                    })

                    It("Should enable outgoing transfers of that partition", func() {
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeTrue())
                    })
                })

                Context("And the node does not hold a replica of that partition", func() {
                    It("Should ensure that partition has been added to the partition pool", func() {
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ }))
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ 1: true }))
                    })

                    It("Should read lock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeTrue())
                    })

                    It("Should write unlock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        nodeFacade.LockPartitionWrites(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeFalse())
                    })

                    It("Should disable outgoing transfers of that partition", func() {
                        // Enable to ensure that UpdatePartition() creates an observable change
                        nodeFacade.EnableOutgoingTransfers(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeFalse())
                    })
                })
            })

            Context("When a node does not own a replica of that partition", func() {
                Context("And the node holds a replica of that partition", func() {
                    BeforeEach(func() {
                        nodeFacade.HeldPartitionReplicaSet().Add(1, 1)
                    })

                    It("Should ensure that partition has been added to the partition pool", func() {
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ }))
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ 1: true }))
                    })

                    It("Should read unlock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        nodeFacade.LockPartitionReads(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeFalse())
                    })

                    It("Should write lock that partition", func() {
                        // Lock to ensure that UpdatePartition() creates an observable change
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeTrue())
                    })

                    It("Should enable outgoing transfers of that partition", func() {
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeTrue())

                    })
                })

                Context("And the node does not hold a replica of that partition", func() {
                    It("Should remove that partition from the partition pool", func() {
                        nodeFacade.AddPartition(1)
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ 1: true }))
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.Partitions()).Should(Equal(map[uint64]bool{ }))
                    })

                    It("Should read lock that partition", func() {
                        nodeFacade.AddPartition(1)
                        nodeFacade.UnlockPartitionReads(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.ReadLockedPartitions()[1]).Should(BeTrue())
                    })

                    It("Should write lock that partition", func() {
                        nodeFacade.AddPartition(1)
                        nodeFacade.UnlockPartitionWrites(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeFalse())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.WriteLockedPartitions()[1]).Should(BeTrue())
                    })

                    It("Should disable outgoing transfers of that partition", func() {
                        nodeFacade.EnableOutgoingTransfers(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeTrue())
                        nodePartitionUpdater.UpdatePartition(1)
                        Expect(nodeFacade.EnabledOutgoingTransfers()[1]).Should(BeFalse())
                    })
                })
            })
        })
    })

    Describe("ClusterNodeStateCoordinator", func() {
        var nodeFacade *MockNodeCoordinatorFacade
        var nodePartitionUpdater *MockClusterNodePartitionUpdater
        var stateCoordinator *ClusterNodeStateCoordinator

        BeforeEach(func() {
            nodeFacade = NewMockNodeCoordinatorFacade(1)
            nodePartitionUpdater = NewMockClusterNodePartitionUpdater()
            stateCoordinator = NewClusterNodeStateCoordinator(nodeFacade, nodePartitionUpdater)
        })

        Describe("#InitializeNodeState", func() {
            Specify("For all partitions that are owned or held by the node, UpdatePartition should be called for it at least once", func() {
                // holds 1, 2, 3
                // owns 0, 2, 4
                nodeFacade.HeldPartitionReplicaSet().Add(1, 0)
                nodeFacade.HeldPartitionReplicaSet().Add(2, 2)
                nodeFacade.HeldPartitionReplicaSet().Add(3, 4)
                nodeFacade.OwnedPartitionReplicaSet().Add(0, 1)
                nodeFacade.OwnedPartitionReplicaSet().Add(2, 3)
                nodeFacade.OwnedPartitionReplicaSet().Add(4, 5)

                stateCoordinator.InitializeNodeState()

                Expect(nodePartitionUpdater.UpdatePartitionCalls(0) > 0).Should(BeTrue())
                Expect(nodePartitionUpdater.UpdatePartitionCalls(1) > 0).Should(BeTrue())
                Expect(nodePartitionUpdater.UpdatePartitionCalls(2) > 0).Should(BeTrue())
                Expect(nodePartitionUpdater.UpdatePartitionCalls(3) > 0).Should(BeTrue())
                Expect(nodePartitionUpdater.UpdatePartitionCalls(4) > 0).Should(BeTrue())
            })

            Specify("A transfer should be started for any partition replica that is owned but not held by the node", func() {
                nodeFacade.HeldPartitionReplicaSet().Add(1, 0)
                nodeFacade.HeldPartitionReplicaSet().Add(2, 2)
                nodeFacade.HeldPartitionReplicaSet().Add(3, 4)
                nodeFacade.HeldPartitionReplicaSet().Add(4, 0)
                nodeFacade.OwnedPartitionReplicaSet().Add(1, 1)
                nodeFacade.OwnedPartitionReplicaSet().Add(2, 2)
                nodeFacade.OwnedPartitionReplicaSet().Add(3, 5)
                nodeFacade.OwnedPartitionReplicaSet().Add(5, 0)

                stateCoordinator.InitializeNodeState()

                Expect(nodeFacade.IncomingTransfers().Map()).Should(Equal(map[uint64]map[uint64]bool{
                    1: map[uint64]bool{
                        1: true,
                    },
                    3: map[uint64]bool{
                        5: true,
                    },
                    5: map[uint64]bool{
                        0: true,
                    },
                }))
            })
        })

        Describe("#ProcessClusterUpdates", func() {
            var deltas []ClusterStateDelta

            Context("deltas include a DeltaNodeAdd", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeAdd, Delta: NodeAdd{ } } }
                })

                Specify("Should call NotifyJoinedCluster() on the node facade", func() {
                    stateCoordinator.ProcessClusterUpdates(deltas)

                    select {
                    case <-nodeFacade.JoinedCluster():
                    default:
                        Fail("Join cluster notification did not happen")
                    }
                })
            })

            Context("deltas include a DeltaNodeRemove", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeRemove, Delta: NodeRemove{ } } }
                })

                Specify("Should call NotifyLeftCluster() on the node facade", func() {
                    stateCoordinator.ProcessClusterUpdates(deltas)

                    select {
                    case <-nodeFacade.LeftCluster():
                    default:
                        Fail("Leave cluster notification did not happen")
                    }
                })
            })

            Context("deltas include a DeltaNodeGainPartitionReplica", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeGainPartitionReplica, Delta: NodeGainPartitionReplica{ Partition: 22, Replica: 45 } } }
                })

                Specify("Should call UpdatePartition() for that partition on the partition updater", func() {
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(0))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(1))
                })
            })

            Context("deltas include a DeltaNodeLosePartitionReplica", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeLosePartitionReplica, Delta: NodeLosePartitionReplica{ Partition: 22, Replica: 45 } } }
                })

                Specify("Should call UpdatePartition() for that partition on the partition updater", func() {
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(0))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(1))
                })
            })

            Context("deltas include a DeltaNodeGainPartitionReplicaOwnership", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeGainPartitionReplicaOwnership, Delta: NodeGainPartitionReplicaOwnership{ Partition: 22, Replica: 45 } } }
                })

                Specify("Should call UpdatePartition() for that partition on the partition updater", func() {
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(0))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(1))
                })

                Specify("Should call StartIncomingTransfer() for that partition replica on the node facade", func() {
                    Expect(nodeFacade.IncomingTransfers().Replicas(22)).Should(Equal([]uint64{ }))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodeFacade.IncomingTransfers().Replicas(22)).Should(Equal([]uint64{ 45 }))
                })
            })

            Context("deltas include a DeltaNodeLosePartitionReplicaOwnership", func() {
                BeforeEach(func() {
                    deltas = []ClusterStateDelta{ ClusterStateDelta{ Type: DeltaNodeLosePartitionReplicaOwnership, Delta: NodeLosePartitionReplicaOwnership{ Partition: 22, Replica: 45 } } }
                })

                Specify("Should call UpdatePartition() for that partition on the partition updater", func() {
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(0))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodePartitionUpdater.UpdatePartitionCalls(22)).Should(Equal(1))
                })

                Specify("Should call StopIncomingTransfer() for that partition replica on the node facade", func() {
                    nodeFacade.IncomingTransfers().Add(22, 45)
                    Expect(nodeFacade.IncomingTransfers().Replicas(22)).Should(Equal([]uint64{ 45 }))
                    stateCoordinator.ProcessClusterUpdates(deltas)
                    Expect(nodeFacade.IncomingTransfers().Replicas(22)).Should(Equal([]uint64{ }))
                })
            })

            Context("deltas include a DeltaSiteAdded", func() {
            })

            Context("deltas include a DeltaSiteRemoved", func() {
            })

            Context("deltas include a DeltaRelayAdded", func() {
            })

            Context("deltas include a DeltaRelayRemoved", func() {
            })

            Context("deltas include a DeltaRelayMoved", func() {
            })
        })
    })

    // integration tests
    Describe("ClusterNodeStateCoordinator using NodePartitionUpdater", func() {
    })
})
