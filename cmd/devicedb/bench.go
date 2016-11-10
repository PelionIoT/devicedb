package main

import (
    "fmt"
    "os"
    "crypto/rand"
    "encoding/binary"
    "time"
    "sync"
    
    . "devicedb"
)

func init() {
    registerCommand("benchmark", benchmark, benchmarkUsage)
}

var benchmarkUsage string = 
`Usage: devicedb benchmark -db=[scratch space database directory]
`

var benchmarkMagnitude int = 10000
var serverConfig ServerConfig
var server *Server

func benchmark() {
    if len(*optDatabaseDir) == 0 {
        fmt.Fprintf(os.Stderr, "No database directory (-db) specified\n")
        
        return
    }
    
    if *optMerkleDepth < uint64(MerkleMinDepth) || *optMerkleDepth > uint64(MerkleMaxDepth) {
        fmt.Fprintf(os.Stderr, "No valid merkle depth specified. Defaulting to %d\n", MerkleDefaultDepth)
        
        serverConfig.MerkleDepth = MerkleDefaultDepth
    } else {
        serverConfig.MerkleDepth = uint8(*optMerkleDepth)
    }
    
    err := os.RemoveAll(*optDatabaseDir)
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Unable to initialized benchmark workspace at %s: %v\n", *optDatabaseDir, err)
        
        return
    }
    
    SetLoggingLevel("error")
    serverConfig.DBFile = *optDatabaseDir
    server, err = NewServer(serverConfig)
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Unable to initialize test databas: %v\n", err)
        
        return
    }
    
    err = benchmarkSequentialReads()
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed at sequential reads benchmark: %v\n", err)
        
        return
    }
    
    err = benchmarkRandomReads()
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed at random reads benchmark: %v\n", err)
        
        return
    }
    
    err = benchmarkWrites()
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed at writes benchmark: %v\n", err)
        
        return
    }
}

func randomString() string {
    randomBytes := make([]byte, 16)
    rand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%05x%05x", high, low)
}

// test reads per second
func benchmarkSequentialReads() error {
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench1" + randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(randomString() + randomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
        _, err := server.Buckets().Get("default").Node.Batch(updateBatch)
        
        if err != nil {
            return err
        }
    }
    
    iter, err := server.Buckets().Get("default").Node.GetMatches([][]byte{ []byte("key") })
    
    if err != nil {
        return err
    }
    
    defer iter.Release()

    start := time.Now()
    
    for iter.Next() {
    }
    
    if iter.Error() != nil {
        return iter.Error()
    }
    
    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    readsPerSecond := time.Second / average
    
    fmt.Printf("%d sequential reads took %s or an average of %s per read or %d reads per second\n", benchmarkMagnitude, elapsed.String(), average.String(), readsPerSecond)
    
    return nil
}

func benchmarkRandomReads() error {
    keys := make([]string, 0, benchmarkMagnitude)
    
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench2" + randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(randomString() + randomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
        _, err := server.Buckets().Get("default").Node.Batch(updateBatch)
        
        if err != nil {
            return err
        }
        
        keys = append(keys, string(key))
    }

    start := time.Now()
    
    for _, key := range keys {
        _, err := server.Buckets().Get("default").Node.Get([][]byte{ []byte(key) })
        
        if err != nil {
            return err
        }
    }
    
    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    readsPerSecond := time.Second / average
    
    fmt.Printf("%d random reads took %s or an average of %s per read or %d reads per second\n", benchmarkMagnitude, elapsed.String(), average.String(), readsPerSecond)
    
    return nil
}

func benchmarkWrites() error {
    var batchWaits sync.WaitGroup
    var err error
    
    start := time.Now()
    
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench3" + randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(randomString() + randomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
    
        batchWaits.Add(1)
        
        go func() {
            _, e := server.Buckets().Get("default").Node.Batch(updateBatch)
            
            if e != nil {
                err = e
            }
            
            batchWaits.Done()
        }()
    }
    
    batchWaits.Wait()
    
    if err != nil {
        return err
    }

    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    batchesPerSecond := time.Second / average
    
    fmt.Printf("%d writes took %s or an average of %s per write or %d writes per second\n", benchmarkMagnitude, elapsed.String(), average.String(), batchesPerSecond)
    
    return nil
}
