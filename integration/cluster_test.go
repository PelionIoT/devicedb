package integration_test

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "errors"
    "io/ioutil"
    "time"

    . "devicedb/bucket"
    . "devicedb/cluster"
    "devicedb/client"
    . "devicedb/data"
    "devicedb/node"
    "devicedb/server"
    . "devicedb/storage"
    . "devicedb/util"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

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
        Context("Single node cluster", func() {
            var clusterClient *client.APIClient
            var node1Server *server.CloudServer
            var node1 *node.ClusterNode
            var node1Storage StorageDriver

            BeforeEach(func() {
                node1Server = tempServer(8080, 9090)
                node1Storage = tempStorageDriver()
                node1 = node.New(node.ClusterNodeConfig{
                    CloudServer: node1Server,
                    StorageDriver: node1Storage,
                })
                clusterClient = client.New(client.APIClientConfig{ Servers: []string{ "localhost:8080" } })
            })

            Describe("Batch update should be successful and replicate to the single node", func() {
                Specify("It should be replicated to the RF number of nodes", func() {
                    var nodeInitialized chan int = make(chan int)
                    var nodeStopped chan error = make(chan error)

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

                    var err error
                    var update *UpdateBatch = NewUpdateBatch()
                    _, err = update.Put([]byte("a"), []byte("hello"), NewDVV(NewDot("cloud-0", 0), map[string]uint64{ }))

                    Expect(err).Should(Not(HaveOccurred()))

                    err = clusterClient.AddSite(context.TODO(), "site1")

                    Expect(err).Should(Not(HaveOccurred()))

                    <-time.After(time.Second * 1)

                    _, _, err = node1.ClusterIO().Batch(context.TODO(), "site1", "default", update)

                    Expect(err).Should(Not(HaveOccurred()))

                    siblingSets, err := node1.ClusterIO().Get(context.TODO(), "site1", "default", [][]byte{ []byte("a") })

                    Expect(err).Should(Not(HaveOccurred()))
                    Expect(len(siblingSets)).Should(Equal(1))
                    Expect(siblingSets[0].Value()).Should(Equal([]byte("hello")))
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
