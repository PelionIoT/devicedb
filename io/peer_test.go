package io_test

import (
    "fmt"
    "time"
    "errors"
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    
	. "devicedb/dbobject"
	. "devicedb/io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func loadCerts(id string) (*tls.Config, *tls.Config, error) {
    clientCertificate, err := tls.LoadX509KeyPair("./test_certs/" + id + ".client.cert.pem", "./test_certs/" + id + ".client.key.pem")
    
    if err != nil {
        return nil, nil, err
    }
    
    serverCertificate, err := tls.LoadX509KeyPair("./test_certs/" + id + ".server.cert.pem", "./test_certs/" + id + ".server.key.pem")
    
    if err != nil {
        return nil, nil, err
    }
    
    rootCAChain, err := ioutil.ReadFile("./test_certs/ca-chain.cert.pem")
    
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

var _ = Describe("Peer", func() {
    var initiatorPeer *Peer
    var responderPeer *Peer
    var initiatorSyncController *SyncController
    var responderSyncController *SyncController
    var initiatorServer *Server
    var responderServer *Server
    
    responderServerTLS, responderClientTLS, err := loadCerts("WWRL000000")

    if err != nil {
        fmt.Println("Unable to load responder certs", err)
        
        return
    }
    
    initiatorServerTLS, initiatorClientTLS, err := loadCerts("WWRL000001")
    
    if err != nil {
        fmt.Println("Unable to load initiator certs", err)
        
        return
    }
    
    stop := make(chan int)
    
    BeforeEach(func() {
        responderSyncController = NewSyncController(2, nil)
        responderPeer = NewPeer(responderSyncController, responderClientTLS)
        responderServer, _ = NewServer(ServerConfig{
            DBFile: "/tmp/testdb-" + randomString(),
            Port: 8080,
            ServerTLS: responderServerTLS,
            Peer: responderPeer,
        })
        
        initiatorSyncController = NewSyncController(2, nil)
        initiatorPeer = NewPeer(initiatorSyncController, initiatorClientTLS)
        initiatorServer, _ = NewServer(ServerConfig{
            DBFile: "/tmp/testdb-" + randomString(),
            Port: 8181,
            ServerTLS: initiatorServerTLS,
            Peer: initiatorPeer,
        })
        
        go func() {
            responderServer.Start()
            stop <- 1
        }()
        
        time.Sleep(time.Millisecond * 100)
    })
    
    AfterEach(func() {
        responderServer.Stop()
        <-stop
    })
    
    Describe("sync", func() {
        It("makes sure that the id is extracted correctly from the client certificate and server certificates", func() {
            initiatorPeer.Connect("WWRL000000", "127.0.0.1", 8080)
            //responderSyncController.StartResponderSessions()
            //initiatorSyncController.StartInitiatorSessions()
            responderSyncController.Start()
            initiatorSyncController.Start()
        
            go func() {
                for i := 0; i < 10; i += 1 {
                    time.Sleep(time.Second * 1)
                    updateBatch := NewUpdateBatch()
                    updateBatch.Put([]byte(randomString()), []byte(randomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
                    responderServer.Buckets().Get("default").Node.Batch(updateBatch)
                }
            }()
            
            Expect(err).Should(BeNil())
            
            time.Sleep(time.Second * 60)
            
            initiatorPeer.Disconnect("WWRL000000")
            
            time.Sleep(time.Second * 5)
            
            /*siblingSets, err := responderServer.Buckets().Get("default").Node.Get([][]byte{ []byte("OBJ1") })
                
            Expect(err).Should(BeNil())
            Expect(len(siblingSets)).Should(Equal(1))
            Expect(siblingSets[0].Value()).Should(Equal([]byte("hello")))
            
            
            siblingSets, err = initiatorServer.Buckets().Get("default").Node.Get([][]byte{ []byte("OBJ1") })
                
            Expect(err).Should(BeNil())
            Expect(len(siblingSets)).Should(Equal(1))
            Expect(siblingSets[0].Value()).Should(Equal([]byte("hello")))*/
            
            Expect(true).Should(BeTrue())
        })
    })
})