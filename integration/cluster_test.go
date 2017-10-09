package integration_test

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "errors"
    "fmt"
    "io/ioutil"
    "time"

    . "devicedb/bucket"
    . "devicedb/cluster"
    "devicedb/client"
    . "devicedb/data"
    . "devicedb/error"
    "devicedb/node"
    "devicedb/raft"
    "devicedb/server"
    . "devicedb/storage"
    . "devicedb/util"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var nextPort = 9000

func loadCerts(id string) (*tls.Config, *tls.Config, error) {
    clientCertificate, err := tls.LoadX509KeyPair("../test_certs/" + id + ".client.cert.pem", "../test_certs/" + id + ".client.key.pem")
    
    if err != nil {
        return nil, nil, err
    }
    
    serverCertificate, err := tls.LoadX509KeyPair("../test_certs/" + id + ".server.cert.pem", "../test_certs/" + id + ".server.key.pem")
    
    if err != nil {
        return nil, nil, err
    }
    
    rootCAChain, err := ioutil.ReadFile("../test_certs/ca-chain.cert.pem")
    
    if err != nil {
        return nil, nil, err
    }
    
    rootCAs := x509.NewCertPool()
    if !rootCAs.AppendCertsFromPEM(rootCAChain) {
        return nil, nil, errors.New("Could not append certs to chain")
    }
    
    var serverTLSConfig = &tls.Config{
        Certificates: []tls.Certificate{ serverCertificate },
        ClientCAs: rootCAs,
    }
    var clientTLSConfig = &tls.Config{
        Certificates: []tls.Certificate{ clientCertificate },
        RootCAs: rootCAs,
    }
    
    return serverTLSConfig, clientTLSConfig, nil
}

func tempServer(internalPort int, externalPort int) *server.CloudServer {
    // Use relay certificates in place of some cloud certs
    serverTLS, _, err := loadCerts("WWRL000000")

    Expect(err).Should(BeNil())

    return server.NewCloudServer(server.CloudServerConfig{
        ExternalHost: "localhost",
        ExternalPort: externalPort,
        InternalHost: "localhost",
        InternalPort: internalPort,
        NodeID: 1,
        RelayTLSConfig: serverTLS,
    })
}

func tempStorageDriver() StorageDriver {
    return NewLevelDBStorageDriver("/tmp/testdb-" + RandomString(), nil)
}

var _ = Describe("Cluster Operation", func() {
    Describe("Cluster IO", func() {
        Context("In a single node cluster", func() {
            var clusterClient *client.APIClient
            var node1Server *server.CloudServer
            var node1 *node.ClusterNode
            var node1Storage StorageDriver
            var nodeInitialized chan int = make(chan int)
            var nodeStopped chan error = make(chan error)

            BeforeEach(func() {
                node1Server = tempServer(nextPort, nextPort + 1)
                node1Storage = tempStorageDriver()
                node1 = node.New(node.ClusterNodeConfig{
                    CloudServer: node1Server,
                    StorageDriver: node1Storage,
                })
                clusterClient = client.New(client.APIClientConfig{ Servers: []string{ fmt.Sprintf("localhost:%d", nextPort) } })
                go func() {
                    nodeStopped <- node1.Start(node.NodeInitializationOptions{ 
                        StartCluster: true,
                        ClusterSettings: ClusterSettings{
                            Partitions: 4,
                            ReplicationFactor: 3,
                        },
                    })
                }()

                node1.OnInitialized(func() {
                    nodeInitialized <- 1
                })

                select {
                case <-nodeInitialized:
                case <-nodeStopped:
                    Fail("Node was never initialized.")
                }
            })

            AfterEach(func() {
                nextPort += 2
                node1.Stop()

                select {
                case <-nodeStopped:
                case <-time.After(time.Second):
                    Fail("Unable to stop node")
                }
            })

            Describe("Putting a key into a site", func() {
                Context("When that site was added but has since been removed", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        Expect(clusterClient.RemoveSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                    })

                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has not been added", func() {
                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has been added", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                    })

                    Context("And the bucket is not valid", func() {
                        It("Should fail with an EBucketDoesNotExist error", func() {
                            <-time.After(time.Second)
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "badbucket", update)

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = node1.ClusterIO().Get(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))
                        })
                    })

                    Context("And the bucket is valid", func() {
                        It("should succeed in being written to the single node for that site", func() {
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                            Expect(err).Should(Not(HaveOccurred()))

                            siblingSets, err := node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))
                            Expect(siblingSets[0].Value()).Should(Equal([]byte("hello")))

                            siblingSetIterator, err := node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(siblingSetIterator.Next()).Should(BeTrue())
                            Expect(siblingSetIterator.Prefix()).Should(Equal([]byte("a")))
                            Expect(siblingSetIterator.Key()).Should(Equal([]byte("a")))
                            Expect(siblingSetIterator.Value().Value()).Should(Equal([]byte("hello")))
                        })
                    })
                })
            })

            Describe("Deleting a key from a site", func() {
                Context("When that site was added but has since been removed", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        Expect(clusterClient.RemoveSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                    })

                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has not been added", func() {
                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has been added", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))

                        var update *UpdateBatch = NewUpdateBatch()
                        _, err := update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Not(HaveOccurred()))
                    })

                    Context("And the bucket is not valid", func() {
                        It("Should fail with an EBucketDoesNotExist error", func() {
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "badbucket", update)

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = node1.ClusterIO().Get(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = node1.ClusterIO().GetMatches(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))
                        })
                    })

                    Context("And the bucket is valid", func() {
                        It("should succeed in deleting that key from the single node for that site", func() {
                            <-time.After(time.Second)
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()

                            siblingSets, err := node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))

                            _, err = update.Delete([]byte("a"), NewDVV(NewDot("", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                            Expect(err).Should(Not(HaveOccurred()))

                            siblingSets, err = node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))
                            Expect(siblingSets[0].IsTombstoneSet()).Should(BeTrue())
                            Expect(siblingSets[0].Value()).Should(BeNil())

                            siblingSetIterator, err := node1.ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(siblingSetIterator.Next()).Should(BeTrue())
                            Expect(siblingSetIterator.Value().IsTombstoneSet()).Should(BeTrue())
                            Expect(siblingSetIterator.Value().Value()).Should(BeNil())
                        })
                    })
                })
            })
        })

        Describe("Regression test for node snapshot startup", func() {
            var partitions int = 64
            var clusterSize int = 2
            var clusterClient *client.APIClient
            var nodes []*node.ClusterNode
            var nodeInitialized chan int
            var nodeStopped chan error
            var originalRaftLogCompactionSize int = raft.LogCompactionSize

            BeforeEach(func() {
                // This forces compaction to happen often. This means that
                // when the next node is brought up it should not learn
                // of its joining the cluster through a log entry, but rather
                // through the first snapshot received. This was broken in the
                // past. When the snapshot was applied the state deltas were not
                // passed to the local node
                raft.LogCompactionSize = 1
                nodes = make([]*node.ClusterNode, clusterSize)
                servers := make([]string, clusterSize)
                nodeInitialized = make(chan int, clusterSize)
                nodeStopped = make(chan error, clusterSize)

                for i := 0; i < clusterSize; i++ {
                    nodeServer := tempServer(nextPort + (i * 2), nextPort + (i * 2) + 1)
                    nodeStorage := tempStorageDriver()
                    nodes[i] = node.New(node.ClusterNodeConfig{
                        CloudServer: nodeServer,
                        StorageDriver: nodeStorage,
                        MerkleDepth: 4,
                    })

                    servers[i] = fmt.Sprintf("localhost:%d", nextPort + (i * 2))

                    go func(nodeIndex int) {
                        if nodeIndex == 0 {
                            nodeStopped <- nodes[nodeIndex].Start(node.NodeInitializationOptions{ 
                                StartCluster: true,
                                ClusterSettings: ClusterSettings{
                                    Partitions: uint64(partitions),
                                    ReplicationFactor: 3,
                                },
                                ClusterHost: "localhost",
                                ClusterPort: nextPort,
                            })
                        } else {
                            nodeStopped <- nodes[nodeIndex].Start(node.NodeInitializationOptions{
                                JoinCluster: true,
                                SeedNodeHost: "localhost",
                                SeedNodePort: nextPort,
                            })
                        }
                    }(i)

                    nodes[i].OnInitialized(func() {
                        nodeInitialized <- i
                    })
                }

                clusterClient = client.New(client.APIClientConfig{ Servers: servers[:1] })

                for i := 0; i < clusterSize; i++ {
                    select {
                    case <-nodeInitialized:
                    case <-nodeStopped:
                        Fail("Node was never initialized.")
                    }
                }
            })

            AfterEach(func() {
                raft.LogCompactionSize = originalRaftLogCompactionSize
                nextPort += (clusterSize * 2) + 1

                for i := 0; i < clusterSize; i++ {
                    nodes[i].Stop()
                }

                for i := 0; i < clusterSize; i++ {
                    select {
                    case <-nodeStopped:
                    case <-time.After(time.Second):
                        Fail("Unable to stop node")
                    }
                }
            })

            Describe("Cluster node addition snapshot regression test", func() {
                AfterEach(func() {
                    nextPort += 2
                })

                It("Should bring up the new node without hanging", func() {
                    // Add lots of sites. Once site addition = one raft log entry
                    fmt.Println("---------------------------ADDING LOTS OF SITES-------------------------")
                    for i := 0; i < raft.LogCompactionSize * 2; i++ {
                        Expect(clusterClient.AddSite(context.TODO(), fmt.Sprintf("site-%d", i))).Should(Not(HaveOccurred()))
                    }
                    fmt.Println("---------------------------ADDED LOTS OF SITES-------------------------")

                    // Add new node to the cluster
                    nodeServer := tempServer(nextPort + (clusterSize * 2), nextPort + (clusterSize * 2) + 1)
                    nodeStorage := tempStorageDriver()
                    newNode := node.New(node.ClusterNodeConfig{
                        CloudServer: nodeServer,
                        StorageDriver: nodeStorage,
                    })
                    nodeInitialized := make(chan int)
                    nodeStopped := make(chan error)

                    go func() {
                        nodeStopped <- newNode.Start(node.NodeInitializationOptions{
                            JoinCluster: true,
                            SeedNodeHost: "localhost",
                            SeedNodePort: nextPort,
                        })
                    }()

                    newNode.OnInitialized(func() {
                        nodeInitialized <- 1
                    })

                    select {
                    case <-nodeInitialized:
                    case <-nodeStopped:
                        Fail("Node was never initialized.")
                    case <-time.After(time.Second * 10):
                        Fail("Initialization never completed")
                    }

                    //newNode.ClusterConfigController().Pause()


                    fmt.Println("---------------------------ADDING LOTS OF SITES AGAIN-------------------------")
                    <-time.After(time.Second * 5)
                    for i := 0; i < raft.LogCompactionSize; i++ {
                        Expect(clusterClient.AddSite(context.TODO(), fmt.Sprintf("site-%d", i))).Should(Not(HaveOccurred()))
                    }
                    fmt.Println("---------------------------ADDED LOTS OF SITES AGAIN-------------------------")

                    //newNode.ClusterConfigController().Resume()

                    for i := 0; i < partitions; i++ {
                        Expect(newNode.ClusterConfigController().ClusterController().PartitionOwners(uint64(i))).Should(Equal(nodes[0].ClusterConfigController().ClusterController().PartitionOwners(uint64(i))))
                    }

                    Expect(newNode.ClusterConfigController().ClusterController().LocalNodeOwnedPartitionReplicas()).Should(Not(BeEmpty()))
                })
            })
        })

        Context("In a multi node cluster", func() {
            var partitions int = 16
            var clusterSize int = 3
            var clusterClient *client.APIClient
            var nodes []*node.ClusterNode
            var nodeInitialized chan int
            var nodeStopped chan error

            BeforeEach(func() {
                nodes = make([]*node.ClusterNode, clusterSize)
                servers := make([]string, clusterSize)
                nodeInitialized = make(chan int, clusterSize)
                nodeStopped = make(chan error, clusterSize)

                for i := 0; i < clusterSize; i++ {
                    nodeServer := tempServer(nextPort + (i * 2), nextPort + (i * 2) + 1)
                    nodeStorage := tempStorageDriver()
                    nodes[i] = node.New(node.ClusterNodeConfig{
                        CloudServer: nodeServer,
                        StorageDriver: nodeStorage,
                        MerkleDepth: 4,
                    })

                    servers[i] = fmt.Sprintf("localhost:%d", nextPort + (i * 2))

                    go func(nodeIndex int) {
                        if nodeIndex == 0 {
                            nodeStopped <- nodes[nodeIndex].Start(node.NodeInitializationOptions{ 
                                StartCluster: true,
                                ClusterSettings: ClusterSettings{
                                    Partitions: uint64(partitions),
                                    ReplicationFactor: 2,
                                },
                                ClusterHost: "localhost",
                                ClusterPort: nextPort,
                            })
                        } else {
                            nodeStopped <- nodes[nodeIndex].Start(node.NodeInitializationOptions{
                                JoinCluster: true,
                                SeedNodeHost: "localhost",
                                SeedNodePort: nextPort,
                            })
                        }
                    }(i)

                    nodes[i].OnInitialized(func() {
                        nodeInitialized <- i
                    })
                }

                clusterClient = client.New(client.APIClientConfig{ Servers: servers })

                for i := 0; i < clusterSize; i++ {
                    select {
                    case <-nodeInitialized:
                    case <-nodeStopped:
                        Fail("Node was never initialized.")
                    }
                }
            })

            AfterEach(func() {
                nextPort += (clusterSize * 2) + 1

                for i := 0; i < clusterSize; i++ {
                    nodes[i].Stop()
                }

                for i := 0; i < clusterSize; i++ {
                    select {
                    case <-nodeStopped:
                    case <-time.After(time.Second):
                        Fail("Unable to stop node")
                    }
                }
            })

            Describe("Partition transfers after adding a node", func() {
                BeforeEach(func() {
                    // With any luck there will be one or more sites per partition
                    fmt.Println("Adding 1000 sites to the cluster")
                    for i := 0; i < 1000; i++ {
                        Expect(clusterClient.AddSite(context.TODO(), fmt.Sprintf("site%d", i))).Should(Not(HaveOccurred()))
                    }

                    fmt.Println("Waiting for the cluster nodes to catch up")
                    <-time.After(time.Second * 10)
                    fmt.Println("Writing keys to each site")

                    for i := 0; i < 1000; i++ {
                        var siteID string = fmt.Sprintf("site%d", i)

                        for j := 0; j < 10; j++ {
                            var key string = fmt.Sprintf("key-%d", j)

                            fmt.Printf("Writing key %s to site %s\n", key, siteID)

                            var update *UpdateBatch = NewUpdateBatch()
                            _, err := update.Put([]byte(key), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))
                            _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), siteID, "default", update)
                            Expect(err).Should(Not(HaveOccurred()))
                        }
                    }
                })

                AfterEach(func() {
                    nextPort += 2
                })

                Specify("The new node should be assigned some partitions and then have the keys from those partitions sent to it", func() {
                    nodeServer := tempServer(nextPort + (clusterSize * 2), nextPort + (clusterSize * 2) + 1)
                    nodeStorage := tempStorageDriver()
                    newNode := node.New(node.ClusterNodeConfig{
                        CloudServer: nodeServer,
                        StorageDriver: nodeStorage,
                        MerkleDepth: 4,
                    })
                    nodeInitialized := make(chan int)
                    nodeStopped := make(chan error)

                    go func() {
                        nodeStopped <- newNode.Start(node.NodeInitializationOptions{
                            JoinCluster: true,
                            SeedNodeHost: "localhost",
                            SeedNodePort: nextPort,
                        })
                    }()

                    newNode.OnInitialized(func() {
                        nodeInitialized <- 1
                    })

                    select {
                    case <-nodeInitialized:
                    case <-nodeStopped:
                        Fail("Node was never initialized.")
                    case <-time.After(time.Second * 10):
                        Fail("Initialization never completed")
                    }

                    fmt.Println("The new node has been brough online")

                    <-time.After(time.Second * 10)

                    var owned map[uint64]bool = make(map[uint64]bool)
                    var sites map[string]bool = make(map[string]bool)

                    for _, partitionReplica := range newNode.ClusterConfigController().ClusterController().LocalNodeOwnedPartitionReplicas() {
                        owned[partitionReplica.Partition] = true
                    }

                    Expect(owned).Should(Not(BeEmpty()))

                    // Iterate through all sites and see which ones it should own
                    for i := 0; i < 1000; i++ {
                        if owned[newNode.ClusterConfigController().ClusterController().Partition(fmt.Sprintf("site%d", i))] {
                            sites[fmt.Sprintf("site%d", i)] = true
                        }
                    }

                    Expect(sites).Should(Not(BeEmpty()))

                    for siteID, _ := range sites {
                        keys := make([][]byte, 0)
                        partition := newNode.ClusterConfigController().ClusterController().Partition(siteID)

                        for i := 0; i < 10; i++ {
                            keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
                        }

                        siblingSets, err := newNode.Get(context.TODO(), partition, siteID, "default", keys)

                        Expect(err).Should(Not(HaveOccurred()))
                        Expect(siblingSets).Should(HaveLen(10))

                        for _, siblingSet := range siblingSets {
                            Expect(siblingSet).Should(Not(BeNil()))
                            Expect(siblingSet.Value()).Should(Equal([]byte("hello")))
                        }

                        fmt.Printf("New node has all data for site %s\n", siteID)
                    }
                })
            })

            Specify("Bringing up multiple nodes at once should eventually work", func() {
            })

            Describe("Making enough cluster configuration updates so that compaction occurs and then bringing on another node so it receives a snapshot", func() {
                AfterEach(func() {
                    nextPort += 2
                })

                It("Should allow the new node to be brough into the cluster successfully and it should have a consistent snapshot of the cluster state", func() {
                    // Add lots of sites. Once site addition = one raft log entry
                    fmt.Println("---------------------------ADDING LOTS OF SITES-------------------------")
                    for i := 0; i < raft.LogCompactionSize * 2; i++ {
                        Expect(clusterClient.AddSite(context.TODO(), fmt.Sprintf("site-%d", i))).Should(Not(HaveOccurred()))
                    }
                    fmt.Println("---------------------------ADDED LOTS OF SITES-------------------------")

                    // Add new node to the cluster
                    nodeServer := tempServer(nextPort + (clusterSize * 2), nextPort + (clusterSize * 2) + 1)
                    nodeStorage := tempStorageDriver()
                    newNode := node.New(node.ClusterNodeConfig{
                        CloudServer: nodeServer,
                        StorageDriver: nodeStorage,
                    })
                    nodeInitialized := make(chan int)
                    nodeStopped := make(chan error)

                    go func() {
                        nodeStopped <- newNode.Start(node.NodeInitializationOptions{
                            JoinCluster: true,
                            SeedNodeHost: "localhost",
                            SeedNodePort: nextPort,
                        })
                    }()

                    newNode.OnInitialized(func() {
                        nodeInitialized <- 1
                    })

                    select {
                    case <-nodeInitialized:
                    case <-nodeStopped:
                        Fail("Node was never initialized.")
                    }

                    //newNode.ClusterConfigController().Pause()


                    fmt.Println("---------------------------ADDING LOTS OF SITES AGAIN-------------------------")
                    <-time.After(time.Second * 5)
                    for i := 0; i < raft.LogCompactionSize; i++ {
                        Expect(clusterClient.AddSite(context.TODO(), fmt.Sprintf("site-%d", i))).Should(Not(HaveOccurred()))
                    }
                    fmt.Println("---------------------------ADDED LOTS OF SITES AGAIN-------------------------")

                    //newNode.ClusterConfigController().Resume()

                    for i := 0; i < partitions; i++ {
                        Expect(newNode.ClusterConfigController().ClusterController().PartitionOwners(uint64(i))).Should(Equal(nodes[0].ClusterConfigController().ClusterController().PartitionOwners(uint64(i))))
                    }

                    Expect(newNode.ClusterConfigController().ClusterController().LocalNodeOwnedPartitionReplicas()).Should(Not(BeEmpty()))
                })
            })

            Describe("Partition distribution", func() {
                Specify("Each node should be responsible for a roughly equal number of partitions", func() {
                    <-time.After(time.Second * 5)
                    for i := 0; i < clusterSize; i++ {
                        fmt.Printf("Node %d owns %d partition replicas\n", nodes[i].ID(), len(nodes[i].ClusterConfigController().ClusterController().LocalNodeOwnedPartitionReplicas()))
                        fmt.Printf("Node %d tokens %v\n", nodes[i].ID(), nodes[i].ClusterConfigController().ClusterController().State.Nodes[nodes[i].ID()].Tokens)
                        fmt.Printf("Cluster d tokens %v\n", nodes[i].ClusterConfigController().ClusterController().State.Tokens)
                    }
                    <-time.After(time.Second * 5)
                    Fail("The distribution is not yet equal. Need to optimize the partitioning algorithm")
                })
            })

            Describe("Putting a key into a site multiple nodes", func() {
                Context("When that site was added but has since been removed", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        Expect(clusterClient.RemoveSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                    })

                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has not been added", func() {
                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has been added", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        <-time.After(time.Second * 5)
                    })

                    Context("And the bucket is not valid", func() {
                        It("Should fail with an EBucketDoesNotExist error", func() {
                            <-time.After(time.Second)
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "badbucket", update)

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))
                        })
                    })

                    Context("And the bucket is valid", func() {
                        It("should succeed in being written to a quorum of nodes for that site", func() {
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                            Expect(err).Should(Not(HaveOccurred()))

                            siblingSets, err := nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))
                            Expect(siblingSets[0].IsTombstoneSet()).Should(BeFalse())

                            for sibling := range siblingSets[0].Iter() {
                                Expect(sibling.Value()).Should(Equal([]byte("hello")))
                            }

                            siblingSetIterator, err := nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(siblingSetIterator.Next()).Should(BeTrue())
                            Expect(siblingSetIterator.Prefix()).Should(Equal([]byte("a")))
                            Expect(siblingSetIterator.Key()).Should(Equal([]byte("a")))

                            Expect(siblingSetIterator.Value().IsTombstoneSet()).Should(BeFalse())

                            for sibling := range siblingSetIterator.Value().Iter() {
                                Expect(sibling.Value()).Should(Equal([]byte("hello")))
                            }
                        })
                    })
                })
            })

            Describe("Deleting a key from a site multiple nodes", func() {
                Context("When that site was added but has since been removed", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        Expect(clusterClient.RemoveSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))
                        <-time.After(time.Second * 5)
                    })

                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has not been added", func() {
                    It("Should fail with an ESiteDoesNotExist error", func() {
                        // This timeout should work most of the time. May fail if partition transfers don't complete before this test is started
                        <-time.After(time.Second)
                        var err error
                        var update *UpdateBatch = NewUpdateBatch()
                        _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))

                        _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                        Expect(err).Should(Equal(ESiteDoesNotExist))
                    })
                })

                Context("When that site has been added", func() {
                    BeforeEach(func() {
                        Expect(clusterClient.AddSite(context.TODO(), "site1")).Should(Not(HaveOccurred()))

                        <-time.After(time.Second * 5)

                        var update *UpdateBatch = NewUpdateBatch()
                        _, err := update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                        Expect(err).Should(Not(HaveOccurred()))

                        _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                        Expect(err).Should(Not(HaveOccurred()))
                    })

                    Context("And the bucket is not valid", func() {
                        It("Should fail with an EBucketDoesNotExist error", func() {
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()
                            _, err = update.Delete([]byte("a"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "badbucket", update)

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))

                            _, err = nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "badbucket", [][]byte{ []byte("a") })

                            Expect(err).Should(Equal(EBucketDoesNotExist))
                        })
                    })

                    Context("And the bucket is valid", func() {
                        Context("But one or more of the replica nodes is down", func() {
                            It("Should return an ENoQuorum error", func() {
                                var err error
                                var update *UpdateBatch = NewUpdateBatch()

                                siblingSets, err := nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                                Expect(err).Should(Not(HaveOccurred()))
                                Expect(len(siblingSets)).Should(Equal(1))

                                _, err = update.Delete([]byte("a"), NewDVV(NewDot("", 0), map[string]uint64{ }))

                                Expect(err).Should(Not(HaveOccurred()))

                                fmt.Println("Shutting down nodes")
                                nodes[1].Stop()
                                nodes[2].Stop()
                                <-time.After(time.Second * 5)
                                fmt.Println("Shut down nodes. Now will attempt to do batch but should fail with ENoQuorum")

                                _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                                Expect(err).Should(Equal(ENoQuorum))
                            })
                        })

                        It("should succeed in deleting that key from the single node for that site", func() {
                            <-time.After(time.Second)
                            var err error
                            var update *UpdateBatch = NewUpdateBatch()

                            siblingSets, err := nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))

                            _, err = update.Delete([]byte("a"), NewDVV(NewDot("", 0), map[string]uint64{ }))

                            Expect(err).Should(Not(HaveOccurred()))

                            _, _, err = nodes[0].ClusterIO().Batch(context.TODO(), "site1", "default", update)

                            Expect(err).Should(Not(HaveOccurred()))

                            siblingSets, err = nodes[0].ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(len(siblingSets)).Should(Equal(1))
                            Expect(siblingSets[0].IsTombstoneSet()).Should(BeTrue())
                            Expect(siblingSets[0].Value()).Should(BeNil())

                            siblingSetIterator, err := nodes[0].ClusterIO().GetMatches(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                            Expect(err).Should(Not(HaveOccurred()))
                            Expect(siblingSetIterator.Next()).Should(BeTrue())
                            Expect(siblingSetIterator.Value().IsTombstoneSet()).Should(BeTrue())
                            Expect(siblingSetIterator.Value().Value()).Should(BeNil())
                        })
                    })
                })
            })
        })
    })

    Describe("Cluster Membership", func() {
        Describe("Adding Nodes", func() {
        })

        Describe("Removing Nodes", func() {
        })

        Describe("Decomissioning Nodes", func() {
        })
    })
})
