package devicedb

import (
    "io"
    "net"
    "net/http"
    "encoding/json"
    "github.com/gorilla/mux"
)

type Shared struct {
}

func (shared *Shared) ShouldReplicateOutgoing(peerID string) bool {
    return true
}

func (shared *Shared) ShouldReplicateIncoming(peerID string) bool {
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

type Server struct {
    bucketList *BucketList
    httpServer *http.Server
    listener net.Listener
}

func NewServer() *Server {
    server := &Server{ NewBucketList(), nil, nil }
    storageDriver := NewLevelDBStorageDriver("/tmp/testdevicedb", nil)
    nodeID := "nodeA"
    
    defaultNode, _ := NewNode(nodeID, &PrefixedStorageDriver{ []byte{ 0 }, storageDriver }, MerkleDefaultDepth, nil)
    cloudNode, _ := NewNode(nodeID, &PrefixedStorageDriver{ []byte{ 1 }, storageDriver }, MerkleDefaultDepth, nil) 
    lwwNode, _ := NewNode(nodeID, &PrefixedStorageDriver{ []byte{ 2 }, storageDriver }, MerkleDefaultDepth, lastWriterWins)
    
    server.bucketList.AddBucket("default", defaultNode, &Shared{ })
    server.bucketList.AddBucket("lww", lwwNode, &Shared{ })
    server.bucketList.AddBucket("cloud", cloudNode, &Cloud{ })
    
    return server
}

func (server *Server) Start() error {
    server.Stop()
    
    r := mux.NewRouter()
    
    r.HandleFunc("/{bucket}/values", func(w http.ResponseWriter, r *http.Request) {
        bucket := mux.Vars(r)["bucket"]
        
        if !server.bucketList.HasBucket(bucket) {
            log.Warningf("POST /{bucket}/values: Invalid bucket")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
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
        
        siblingSetsJSON, _ := json.Marshal(siblingSets)
        
        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, string(siblingSetsJSON) + "\n")
    }).Methods("POST")
    
    server.httpServer = &http.Server{
        Handler: r,
        WriteTimeout: 15000,
        ReadTimeout: 15000,
    }
    
    listener, err := net.Listen("tcp", ":8080")
    
    if err != nil {
        return err
    }
    
    server.listener = listener
    
    return server.httpServer.Serve(server.listener)
}

func (server *Server) Stop() error {
    if server.listener != nil {
        server.listener.Close()
    }
    
    return nil
}