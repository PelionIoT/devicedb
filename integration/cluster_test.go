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

        Context("In a multi node cluster", func() {
            var clusterSize int = 5
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
                    })

                    servers[i] = fmt.Sprintf("localhost:%d", nextPort + (i * 2))

                    go func(nodeIndex int) {
                        if nodeIndex == 0 {
                            nodeStopped <- nodes[nodeIndex].Start(node.NodeInitializationOptions{ 
                                StartCluster: true,
                                ClusterSettings: ClusterSettings{
                                    Partitions: 4,
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

            It("Should work", func() {

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
