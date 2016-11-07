package devicedb

import (
    "fmt"
    "io"
    "net"
    "errors"
    "net/http"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "encoding/hex"
    "time"
    "strconv"
    "github.com/gorilla/mux"
    "github.com/gorilla/websocket"
)

type Shared struct {
}

func (shared *Shared) ShouldReplicateOutgoing(peerID string) bool {
    return true
}

func (shared *Shared) ShouldReplicateIncoming(peerID string) bool {
    return true
}

func (shared *Shared) ShouldAcceptReads(clientID string) bool {
    return true
}

func (shared *Shared) ShouldAcceptWrites(clientID string) bool {
    return true
}

type Cloud struct {
}

func (cloud *Cloud) ShouldReplicateOutgoing(peerID string) bool {
    return false
}

func (cloud *Cloud) ShouldReplicateIncoming(peerID string) bool {
    return peerID == "cloud"
}

func (cloud *Cloud) ShouldAcceptReads(clientID string) bool {
    return true
}

func (cloud *Cloud) ShouldAcceptWrites(clientID string) bool {
    return false
}

type Local struct {
}

func (local *Local) ShouldReplicateOutgoing(peerID string) bool {
    return false
}

func (local *Local) ShouldReplicateIncoming(peerID string) bool {
    return false
}

type peerAddress struct {
    id string
    host string
    port int
}

type ServerConfig struct {
    DBFile string
    Port int
    MerkleDepth uint8
    NodeID string
    Peer *Peer
    ServerTLS *tls.Config
    PeerAddresses map[string]peerAddress
    SyncPushBroadcastLimit uint64
}

func (sc *ServerConfig) LoadFromFile(file string) error {
    var ysc YAMLServerConfig
    
    err := ysc.LoadFromFile(file)
    
    if err != nil {
        return err
    }
    
    sc.DBFile = ysc.DBFile
    sc.Port = ysc.Port
    sc.MerkleDepth = ysc.MerkleDepth
    sc.SyncPushBroadcastLimit = ysc.SyncPushBroadcastLimit
    sc.PeerAddresses = make(map[string]peerAddress)

    rootCAs := x509.NewCertPool()
    
    if !rootCAs.AppendCertsFromPEM([]byte(ysc.TLS.RootCA)) {
        return errors.New("Could not append root CA to chain")
    }
    
    clientCertificate, _ := tls.X509KeyPair([]byte(ysc.TLS.ClientCertificate), []byte(ysc.TLS.ClientKey))
    serverCertificate, _ := tls.X509KeyPair([]byte(ysc.TLS.ServerCertificate), []byte(ysc.TLS.ServerKey))
    clientTLSConfig := &tls.Config{
        Certificates: []tls.Certificate{ clientCertificate },
        RootCAs: rootCAs,
    }
    serverTLSConfig := &tls.Config{
        Certificates: []tls.Certificate{ serverCertificate },
        ClientCAs: rootCAs,
    }
    
    sc.Peer = NewPeer(NewSyncController(uint(ysc.MaxSyncSessions), nil, ysc.SyncSessionPeriod), clientTLSConfig)
    sc.ServerTLS = serverTLSConfig
    
    for _, yamlPeer := range ysc.Peers {
        if _, ok := sc.PeerAddresses[yamlPeer.ID]; ok {
            return errors.New(fmt.Sprintf("Duplicate entry for peer %s in config file", yamlPeer.ID))
        }
        
        sc.PeerAddresses[yamlPeer.ID] = peerAddress{
            id: yamlPeer.ID,
            host: yamlPeer.Host,
            port: yamlPeer.Port,
        }
    }
    
    clientCertX509, _ := x509.ParseCertificate(clientCertificate.Certificate[0])
    serverCertX509, _ := x509.ParseCertificate(serverCertificate.Certificate[0])
    clientCN := clientCertX509.Subject.CommonName
    serverCN := serverCertX509.Subject.CommonName
    
    if len(clientCN) == 0 {
        return errors.New("The common name in the certificate is empty. The node ID must not be empty")
    }
     
    if clientCN != serverCN {
        return errors.New(fmt.Sprintf("Server and client certificates have differing common names(%s and %s). This is the string used to uniquely identify the node.", serverCN, clientCN))
    }
    
    sc.NodeID = clientCN
    
    return nil
}

type Server struct {
    bucketList *BucketList
    httpServer *http.Server
    listener net.Listener
    storageDriver StorageDriver
    port int
    upgrader websocket.Upgrader
    peer *Peer
    serverTLS *tls.Config
    id string
    syncPushBroadcastLimit uint64
}

func NewServer(serverConfig ServerConfig) (*Server, error) {
    if serverConfig.MerkleDepth < MerkleMinDepth || serverConfig.MerkleDepth > MerkleMaxDepth {
        serverConfig.MerkleDepth = MerkleDefaultDepth
    }
    
    if len(serverConfig.NodeID) == 0 {
        serverConfig.NodeID = "Node"
    }
    
    upgrader := websocket.Upgrader{
    	ReadBufferSize:  1024,
    	WriteBufferSize: 1024,
    }
    
    storageDriver := NewLevelDBStorageDriver(serverConfig.DBFile, nil)
    nodeID := serverConfig.NodeID
    server := &Server{ NewBucketList(), nil, nil, storageDriver, serverConfig.Port, upgrader, serverConfig.Peer, serverConfig.ServerTLS, nodeID, serverConfig.SyncPushBroadcastLimit }
    err := server.storageDriver.Open()
    
    if err != nil {
        log.Errorf("Error creating server: %v", err)
        return nil, err
    }
    
    defaultNode, _ := NewNode(nodeID, NewPrefixedStorageDriver([]byte{ 0 }, storageDriver), serverConfig.MerkleDepth, nil)
    cloudNode, _ := NewNode(nodeID, NewPrefixedStorageDriver([]byte{ 1 }, storageDriver), serverConfig.MerkleDepth, nil) 
    lwwNode, _ := NewNode(nodeID, NewPrefixedStorageDriver([]byte{ 2 }, storageDriver), serverConfig.MerkleDepth, LastWriterWins)
    localNode, _ := NewNode(nodeID, NewPrefixedStorageDriver([]byte{ 3 }, storageDriver), MerkleMinDepth, nil)
    
    server.bucketList.AddBucket("default", defaultNode, &Shared{ }, &Shared{ })
    server.bucketList.AddBucket("lww", lwwNode, &Shared{ }, &Shared{ })
    server.bucketList.AddBucket("cloud", cloudNode, &Cloud{ }, &Cloud{ })
    server.bucketList.AddBucket("local", localNode, &Local{ }, &Shared{ })
    
    if server.peer != nil && server.peer.syncController != nil {
        server.peer.syncController.buckets = server.bucketList
    }
    
    if server.peer != nil && serverConfig.PeerAddresses != nil {
        for _, pa := range serverConfig.PeerAddresses {
            server.peer.Connect(pa.id, pa.host, pa.port)
        }
    }
    
    return server, nil
}

func (server *Server) Port() int {
    return server.port
}

func (server *Server) Buckets() *BucketList {
    return server.bucketList
}

func (server *Server) Start() error {
    r := mux.NewRouter()
    
    r.HandleFunc("/{bucket}/merkleRoot", func(w http.ResponseWriter, r *http.Request) {
        bucket := mux.Vars(r)["bucket"]
        
        if !server.bucketList.HasBucket(bucket) {
            log.Warningf("POST /{bucket}/values: Invalid bucket")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, string(EInvalidBucket.JSON()) + "\n")
            
            return
        }
        
        hashBytes := server.bucketList.Get(bucket).Node.MerkleTree().RootHash().Bytes()
        
        w.Header().Set("Content-Type", "text/plain; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, hex.EncodeToString(hashBytes[:]))
    }).Methods("GET")
    
    r.HandleFunc("/{bucket}/values", func(w http.ResponseWriter, r *http.Request) {
        bucket := mux.Vars(r)["bucket"]
        
        if !server.bucketList.HasBucket(bucket) {
            log.Warningf("POST /{bucket}/values: Invalid bucket")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, string(EInvalidBucket.JSON()) + "\n")
            
            return
        }
        
        keys := make([]string, 0)
        decoder := json.NewDecoder(r.Body)
        err := decoder.Decode(&keys)
        
        if err != nil {
            log.Warningf("POST /{bucket}/values: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
            
            return
        }
        
        if len(keys) == 0 {
            log.Warningf("POST /{bucket}/values: Empty keys array")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
            
            return
        }
        
        byteKeys := make([][]byte, 0, len(keys))
        
        for _, k := range keys {
            if len(k) == 0 {
                log.Warningf("POST /{bucket}/values: Empty key")
            
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadRequest)
                io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
                
                return
            }
            
            byteKeys = append(byteKeys, []byte(k))
        }
        
        siblingSets, err := server.bucketList.Get(bucket).Node.Get(byteKeys)
        
        if err != nil {
            log.Warningf("POST /{bucket}/values: Internal server error")
        
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
            
            return
        }
        
        transportSiblingSets := make([]*TransportSiblingSet, 0, len(siblingSets))
        
        for _, siblingSet := range siblingSets {
            if siblingSet == nil {
                transportSiblingSets = append(transportSiblingSets, nil)
                
                continue
            }
            
            var transportSiblingSet TransportSiblingSet
            err := transportSiblingSet.FromSiblingSet(siblingSet)
            
            if err != nil {
                log.Warningf("POST /{bucket}/values: Internal server error")
        
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusInternalServerError)
                io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
                
                return
            }
            
            transportSiblingSets = append(transportSiblingSets, &transportSiblingSet)
        }
        
        siblingSetsJSON, _ := json.Marshal(transportSiblingSets)
        
        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, string(siblingSetsJSON))
    }).Methods("POST")
    
    r.HandleFunc("/{bucket}/matches", func(w http.ResponseWriter, r *http.Request) {
        bucket := mux.Vars(r)["bucket"]
        
        if !server.bucketList.HasBucket(bucket) {
            log.Warningf("POST /{bucket}/matches: Invalid bucket")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, string(EInvalidBucket.JSON()) + "\n")
            
            return
        }    
        
        keys := make([]string, 0)
        decoder := json.NewDecoder(r.Body)
        err := decoder.Decode(&keys)
        
        if err != nil {
            log.Warningf("POST /{bucket}/matches: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
            
            return
        }
        
        if len(keys) == 0 {
            log.Warningf("POST /{bucket}/matches: Empty keys array")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
            
            return
        }
        
        byteKeys := make([][]byte, 0, len(keys))
        
        for _, k := range keys {
            if len(k) == 0 {
                log.Warningf("POST /{bucket}/matches: Empty key")
            
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadRequest)
                io.WriteString(w, string(EInvalidKey.JSON()) + "\n")
                
                return
            }
            
            byteKeys = append(byteKeys, []byte(k))
        }
        
        ssIterator, err := server.bucketList.Get(bucket).Node.GetMatches(byteKeys)
        
        if err != nil {
            log.Warningf("POST /{bucket}/matches: Internal server error")
        
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
            
            return
        }
        
        defer ssIterator.Release()
    
        flusher, _ := w.(http.Flusher)
        
        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.Header().Set("X-Content-Type-Options", "nosniff")
        w.WriteHeader(http.StatusOK)
        
        for ssIterator.Next() {
            key := ssIterator.Key()
            prefix := ssIterator.Prefix()
            nextSiblingSet := ssIterator.Value()
            
            if nextSiblingSet.IsTombstoneSet() {
                continue
            }
            
            var nextTransportSiblingSet TransportSiblingSet
            
            err := nextTransportSiblingSet.FromSiblingSet(nextSiblingSet)
            
            if err != nil {
                log.Warningf("POST /{bucket}/matches: Internal server error")
        
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusInternalServerError)
                io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
                
                return
            }
            
            siblingSetsJSON, _ := json.Marshal(&nextTransportSiblingSet)
            
            _, err = fmt.Fprintf(w, "%s\n%s\n%s\n", string(prefix), string(key), string(siblingSetsJSON))
            flusher.Flush()
            
            if err != nil {
                return
            }
        }
    }).Methods("POST")
    
    r.HandleFunc("/{bucket}/batch", func(w http.ResponseWriter, r *http.Request) {
        bucket := mux.Vars(r)["bucket"]
        
        if !server.bucketList.HasBucket(bucket) {
            log.Warningf("POST /{bucket}/batch: Invalid bucket")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, string(EInvalidBucket.JSON()) + "\n")
            
            return
        }
        
        if !server.bucketList.Get(bucket).PermissionStrategy.ShouldAcceptWrites("") {
            log.Warningf("POST /{bucket}/batch: Attempted to read from %s bucket", bucket)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusUnauthorized)
            io.WriteString(w, string(EUnauthorized.JSON()) + "\n")
            
            return
        }
        
        var updateBatch UpdateBatch
        var transportUpdateBatch TransportUpdateBatch
        decoder := json.NewDecoder(r.Body)
        err := decoder.Decode(&transportUpdateBatch)
        
        if err != nil {
            log.Warningf("POST /{bucket}/batch: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidBatch.JSON()) + "\n")
            
            return
        }
        
        err = transportUpdateBatch.ToUpdateBatch(&updateBatch)
        
        if err != nil {
            log.Warningf("POST /{bucket}/batch: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidBatch.JSON()) + "\n")
            
            return
        }
        
        updatedSiblingSets, err := server.bucketList.Get(bucket).Node.Batch(&updateBatch)
        
        if err != nil {
            log.Warningf("POST /{bucket}/batch: Internal server error")
        
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
            
            return
        }
    
        if server.peer != nil {
            for key, siblingSet := range updatedSiblingSets {
                server.peer.SyncController().BroadcastUpdate(bucket, key, siblingSet, server.syncPushBroadcastLimit)
            }
        }
        
        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")
    
    r.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
        if server.peer == nil {
            // log error
            
            return
        }
        
        conn, err := server.upgrader.Upgrade(w, r, nil)
        
        if err != nil {
            return
        }
        
        server.peer.Accept(conn)
    }).Methods("GET")
    
    server.httpServer = &http.Server{
        Handler: r,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var err error

    if server.serverTLS == nil {
        listener, err = net.Listen("tcp", "0.0.0.0:" + strconv.Itoa(server.Port()))
    } else {
        server.serverTLS.ClientAuth = tls.VerifyClientCertIfGiven
        listener, err = tls.Listen("tcp", "0.0.0.0:" + strconv.Itoa(server.Port()), server.serverTLS)
    }
    
    if err != nil {
        log.Errorf("Error listening on port: %d", server.port)
        
        server.Stop()
        
        return err
    }
    
    err = server.storageDriver.Open()
    
    if err != nil {
        log.Errorf("Error opening storage driver: %v", err)
        
        return EStorage
    }
    
    server.listener = listener

    log.Infof("Node %s listening on port %d", server.id, server.port)
    return server.httpServer.Serve(server.listener)
}

func (server *Server) Stop() error {
    if server.listener != nil {
        server.listener.Close()
    }
    
    server.storageDriver.Close()
    
    return nil
}