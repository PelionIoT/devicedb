package server_test

import (
    "time"
    "context"

    . "devicedb/client"
    . "devicedb/server"
    . "devicedb/util"
    . "devicedb/raft"
    . "devicedb/logging"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("CloudServer", func() {
    var defaultServerConfig = CloudServerConfig{
        ReplicationFactor: 3,
        Partitions: 1024,
        Port: 8080,
        Host: "localhost",
        Capacity: 1,
    }

    Describe("Starting a server", func() {
        Context("When no seed host and no seed port are specified in the server config", func() {
            Specify("should create a new single node cluster", func() {
                config := defaultServerConfig
                config.Store = "/tmp/testdb-" + RandomString()
                server, err := NewCloudServer(config)
                join := make(chan int)

                Expect(err).Should(BeNil())

                server.OnJoinCluster(func() {
                    join <- 1
                })

                go func() {
                    server.Start()
                }()

                <-join

                Expect(server.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server.ClusterController().LocalNodeID: true }))
                server.Stop()
            })
        })

        Context("When a seed host and seed port are specified in the server config", func() {
            Specify("should join an existing cluster", func() {
                config0 := defaultServerConfig
                config1 := defaultServerConfig
                config0.Store = "/tmp/testdb-" + RandomString()
                config1.Store = "/tmp/testdb-" + RandomString()
                config1.Port = 8181
                config1.SeedHost = config0.Host
                config1.SeedPort = config0.Port
                server0, err := NewCloudServer(config0)
                Expect(err).Should(BeNil())
                server1, err := NewCloudServer(config1)
                Expect(err).Should(BeNil())
                join0 := make(chan int)
                join1 := make(chan int)

                server0.OnJoinCluster(func() {
                    join0 <- 1
                })

                server1.OnJoinCluster(func() {
                    join1 <- 1
                })

                go func() {
                    server0.Start()
                }()

                <-join0

                go func() {
                    server1.Start()
                }()

                <-join1
                <-time.After(time.Second)

                Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                Expect(server1.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true, server1.ClusterController().LocalNodeID: true }))
                server0.Stop()
                server1.Stop()
            })

            Context("And the new node's ID is a duplicate of another node that already exists in the cluster", func() {
                Specify("The new node should not be allowed to join the cluster", func() {
                    config0 := defaultServerConfig
                    config1 := defaultServerConfig
                    config0.NodeID = 1
                    config1.NodeID = 1
                    config0.Store = "/tmp/testdb-" + RandomString()
                    config1.Store = "/tmp/testdb-" + RandomString()
                    config1.Port = 8181
                    config1.SeedHost = config0.Host
                    config1.SeedPort = config0.Port
                    server0, err := NewCloudServer(config0)
                    Expect(err).Should(BeNil())
                    server1, err := NewCloudServer(config1)
                    Expect(err).Should(BeNil())
                    join0 := make(chan int)
                    stop1 := make(chan int)

                    Expect(server0.ClusterController().LocalNodeID).Should(Equal(uint64(1)))
                    Expect(server1.ClusterController().LocalNodeID).Should(Equal(uint64(1)))

                    server0.OnJoinCluster(func() {
                        join0 <- 1
                    })

                    server1.OnJoinCluster(func() {
                        Fail("Server 1 should not have been allowed to join the cluster")
                    })

                    go func() {
                        server0.Start()
                    }()

                    <-join0

                    go func() {
                        server1.Start()
                        stop1 <- 1
                    }()

                    // Server 1 should stop because it will encounter un unrecoverable error while trying to join
                    // the cluster since it has a duplicate ID
                    <-stop1
                    <-time.After(time.Second)

                    Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                    Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeFalse())
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true }))
                    server0.Stop()
                    server1.Stop()
                })
            })

            Context("And the new node's ID is a duplicate of another node that was previously removed from the cluster", func() {
                Specify("The new node should not be allowed to join the cluster 2", func() {
                    config0 := defaultServerConfig
                    config1 := defaultServerConfig
                    config2 := defaultServerConfig
                    config0.NodeID = 1
                    config1.NodeID = 2
                    config2.NodeID = 2
                    config0.Store = "/tmp/testdb-" + RandomString()
                    config1.Store = "/tmp/testdb-" + RandomString()
                    config2.Store = "/tmp/testdb-" + RandomString()
                    config1.Port = 8181
                    config1.SeedHost = config0.Host
                    config1.SeedPort = config0.Port
                    config2.Port = 8282
                    config2.SeedHost = config0.Host
                    config2.SeedPort = config0.Port
                    server0, err := NewCloudServer(config0)
                    Expect(err).Should(BeNil())
                    server1, err := NewCloudServer(config1)
                    Expect(err).Should(BeNil())
                    server2, err := NewCloudServer(config2)
                    Expect(err).Should(BeNil())
                    join0 := make(chan int)
                    join1 := make(chan int)
                    stop2 := make(chan int)

                    Expect(server0.ClusterController().LocalNodeID).Should(Equal(uint64(1)))
                    Expect(server1.ClusterController().LocalNodeID).Should(Equal(uint64(2)))
                    Expect(server2.ClusterController().LocalNodeID).Should(Equal(uint64(2)))

                    server0.OnJoinCluster(func() {
                        join0 <- 1
                    })

                    server1.OnJoinCluster(func() {
                        join1 <- 1
                    })

                    go func() {
                        server0.Start()
                    }()

                    <-join0

                    go func() {
                        server1.Start()
                    }()

                    <-join1
                    <-time.After(time.Second)

                    Log.Criticalf("Test Progress: Cluster with 2 nodes has been built")

                    Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                    Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true, server1.ClusterController().LocalNodeID: true }))
                    
                    Log.Criticalf("Test Progress: Preparing to remove node 2 from the cluster")

                    client := NewClient(ClientConfig{ })
                    Expect(client.ForceRemoveNode(context.TODO(), PeerAddress{ Host: config0.Host, Port: config0.Port }, config1.NodeID)).Should(BeNil())
                    <-time.After(time.Second)

                    Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                    Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeFalse())
                    Expect(server1.ClusterController().LocalNodeWasRemovedFromCluster()).Should(BeTrue())
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true }))

                    go func() {
                        server2.Start()
                        stop2 <- 1
                    }()

                    // Server 2 should stop because it will encounter un unrecoverable error while trying to join
                    // the cluster since it has a duplicate ID
                    <-stop2
                    
                    Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                    Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeFalse())
                    Expect(server1.ClusterController().LocalNodeWasRemovedFromCluster()).Should(BeTrue())
                    Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                    Expect(server2.ClusterController().LocalNodeIsInCluster()).Should(BeFalse())

                    server0.Stop()
                    server1.Stop()
                    server2.Stop()
                })
            })
        })

        Context("When that node was already removed from its cluster", func() {
            Specify("should result in the server immediately stopping itself", func() {
                config0 := defaultServerConfig
                config1 := defaultServerConfig
                config0.NodeID = 1
                config1.NodeID = 2
                config0.Store = "/tmp/testdb-" + RandomString()
                config1.Store = "/tmp/testdb-" + RandomString()
                config1.Port = 8181
                config1.SeedHost = config0.Host
                config1.SeedPort = config0.Port
                server0, err := NewCloudServer(config0)
                Expect(err).Should(BeNil())
                server1, err := NewCloudServer(config1)
                Expect(err).Should(BeNil())
                join0 := make(chan int)
                join1 := make(chan int)
                stop1 := make(chan int)

                Expect(server0.ClusterController().LocalNodeID).Should(Equal(uint64(1)))
                Expect(server1.ClusterController().LocalNodeID).Should(Equal(uint64(2)))

                server0.OnJoinCluster(func() {
                    join0 <- 1
                })

                server1.OnJoinCluster(func() {
                    join1 <- 1
                })

                go func() {
                    server0.Start()
                }()

                <-join0

                go func() {
                    server1.Start()
                }()

                <-join1
                <-time.After(time.Second)

                Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                Expect(server0.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true, server1.ClusterController().LocalNodeID: true }))
                
                client := NewClient(ClientConfig{ })
                Expect(client.ForceRemoveNode(context.TODO(), PeerAddress{ Host: config0.Host, Port: config0.Port }, config1.NodeID)).Should(BeNil())
                <-time.After(time.Second)

                Expect(server0.ClusterController().LocalNodeIsInCluster()).Should(BeTrue())
                Expect(server1.ClusterController().LocalNodeIsInCluster()).Should(BeFalse())
                Expect(server1.ClusterController().LocalNodeWasRemovedFromCluster()).Should(BeTrue())
                Expect(server0.ClusterController().ClusterNodes()).Should(Equal(server1.ClusterController().ClusterNodes()))
                Expect(server0.ClusterController().ClusterNodes()).Should(Equal(map[uint64]bool{ server0.ClusterController().LocalNodeID: true }))

                go func() {
                    server1.Start()
                    stop1 <- 1
                }()

                // Server 1 should stop because it has been removed from the cluster
                <-stop1
                
                server0.Stop()
                server1.Stop()
            })
        })
    })

    Describe("Decommissioning a node", func() {
        Context("That node is the only node in the cluster", func() {
        })
    })
})
