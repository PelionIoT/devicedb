package io

import (
    "fmt"
    "io"
    "net"
    "net/http"
    "encoding/json"
    "time"
    "github.com/gorilla/mux"
    "devicedb/storage"
    "devicedb/crstrategies"
    "devicedb/sync"
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
    storageDriver storage.StorageDriver
}

func NewServer(dbFile string) (*Server, error) {
    storageDriver := storage.NewLevelDBStorageDriver(dbFile, nil)
    server := &Server{ NewBucketList(), nil, nil, storageDriver }
    nodeID := "nodeA"
    err := server.storageDriver.Open()
    
    if err != nil {
        log.Errorf("Error creating server: %v", err)
        return nil, err
    }
    
    defaultNode, _ := NewNode(nodeID, storage.NewPrefixedStorageDriver([]byte{ 0 }, storageDriver), sync.MerkleDefaultDepth, nil)
    cloudNode, _ := NewNode(nodeID, storage.NewPrefixedStorageDriver([]byte{ 1 }, storageDriver), sync.MerkleDefaultDepth, nil) 
    lwwNode, _ := NewNode(nodeID, storage.NewPrefixedStorageDriver([]byte{ 2 }, storageDriver), sync.MerkleDefaultDepth, crstrategies.LastWriterWins)
    
    server.bucketList.AddBucket("default", defaultNode, &Shared{ })
    server.bucketList.AddBucket("lww", lwwNode, &Shared{ })
    server.bucketList.AddBucket("cloud", cloudNode, &Cloud{ })
    
    return server, nil
}

func (server *Server) Port() int {
    return 8080
}

func (server *Server) Start() error {
    r := mux.NewRouter()
    
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
        
        siblingSetsJSON, _ := json.Marshal(siblingSets)
        
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
            siblingSetsJSON, _ := json.Marshal(nextSiblingSet)
            
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
        
        var updateBatch UpdateBatch
        err := updateBatch.FromJSON(r.Body)
        
        if err != nil {
            log.Warningf("POST /{bucket}/batch: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EInvalidBatch.JSON()) + "\n")
            
            return
        }
        
        _, err = server.bucketList.Get(bucket).Node.Batch(&updateBatch)
        
        if err != nil {
            log.Warningf("POST /{bucket}/batch: Internal server error")
        
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, string(err.(DBerror).JSON()) + "\n")
            
            return
        }
        
        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")
    
    server.httpServer = &http.Server{
        Handler: r,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    listener, err := net.Listen("tcp", "0.0.0.0:8080")
    
    if err != nil {
        server.Stop()
        
        return err
    }
    
    err = server.storageDriver.Open()
    
    if err != nil {
        log.Errorf("Error opening storage driver: %v", err)
        
        return EStorage
    }
    
    server.listener = listener
    
    return server.httpServer.Serve(server.listener)
}

func (server *Server) Stop() error {
    if server.listener != nil {
        server.listener.Close()
    }
    
    server.storageDriver.Close()
    
    return nil
}