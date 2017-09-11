package node_test

import (
    "context"
    "time"

    . "devicedb/cluster"
    . "devicedb/node"
    . "devicedb/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("ClusterNode", func() {
    Describe("#Start", func() {
        Context("The node has not been assigned an ID yet", func() {
            It("Should generate a new ID for this node", func() {
                Fail("Not implemented")
            })

            Context("And if the ID generation fails", func() {
                It("Should return the error that was returned", func() {
                    Fail("Not implemented")
                })
            })
        })

        Context("The node is not yet part of a cluster", func() {
            Context("And the initialization options are set to create a new cluster", func() {
                It("should create a new cluster and add the node to that cluster", func() {
                    options := NodeInitializationOptions{
                        StartCluster: true,
                        ClusterSettings: ClusterSettings{
                            Partitions: 1024,
                            ReplicationFactor: 3,
                        },
                    }
                    raftStore := NewRaftMemoryStorage()
                    // An empty store indicates a new node that requires initialization
                    Expect(raftStore.IsEmpty()).Should(BeTrue())
                    configController := NewMockConfigController(&ClusterController{ })
                    configControllerBuilder := NewMockClusterConfigControllerBuilder()
                    configControllerBuilder.SetDefaultConfigController(configController)

                    replicationFactorSet := make(chan int)
                    partitionsSet := make(chan int)
                    configController.onClusterCommand(func(ctx context.Context, commandBody interface{}) {
                        defer GinkgoRecover()

                        if c, ok := commandBody.(ClusterSetPartitionCountBody); ok {
                            Expect(c.Partitions).Should(Equal(uint64(1024)))
                            partitionsSet <- 1
                            return
                        }

                        if c, ok := commandBody.(ClusterSetReplicationFactorBody); ok {
                            Expect(c.ReplicationFactor).Should(Equal(uint64(3)))
                            replicationFactorSet <- 1
                            return
                        }

                        Fail("Invalid command type")
                    })

                    clusterNode := New()
                    clusterNode.UseRaftStore(raftStore)
                    clusterNode.UseConfigControllerBuilder(configControllerBuilder)

                    startResult := make(chan error)

                    go func() {
                        startResult <- clusterNode.Start(options)
                    }()

                    // Wait until we hear both replication factor set and partitions set
                    select {
                    case <-partitionsSet:
                        select {
                        case <-replicationFactorSet:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }
                    case <-replicationFactorSet:
                        select {
                        case <-partitionsSet:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }

                    // Wait for node to shut down
                    clusterNode.Stop()

                    select {
                    case r := <-startResult:
                        Expect(r).Should(BeNil())
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }
                })
            })

            Context("And the initialization options are set to join an existing cluster", func() {
                It("should add the node to that cluster", func() {
                    Fail("Not implemented")
                })
            })
        })

        Context("The node is part of a cluster", func() {
            Context("And it has been set to be decomissioned", func() {
                It("should put the node into decomissioning mode", func() {
                    Fail("Not implemented")
                })
            })

            Context("And it has not been set to be decomissioned", func() {
                It("The node should resume its duties as a full cluster member", func() {
                    Fail("Not implemented")
                })
            })
        })

        Context("The node used to be part of a cluster but has since been removed", func() {
            It("Should return ERemoved", func() {
                Fail("Not implemented")
            })
        })
    })
})
