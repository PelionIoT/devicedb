package io_test

import (
	. "devicedb/storage"
	. "devicedb/io"
	. "devicedb/dbobject"
	. "devicedb/sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega" 
    
    "crypto/rand"
    "encoding/binary"
    "fmt"
    "testing"
    "sync"
)

func randomString() string {
    randomBytes := make([]byte, 16)
    rand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%05x%05x", high, low)
}

func makeNewStorageDriver() StorageDriver {
    return NewLevelDBStorageDriver("/tmp/testdb-" + randomString(), nil)
}

var _ = Describe("Node", func() {
    makeNode := func(nodeID string) *Node {
        storageEngine := makeNewStorageDriver()
        storageEngine.Open()
        defer storageEngine.Close()
        node, _ := NewNode(nodeID, storageEngine, MerkleMinDepth, nil)
        
        return node
    }
        
    Describe("#Get", func() {
        It("should return EEmpty if the keys slice is nil", func() {
            node := makeNode("nodeA")
            ss, err := node.Get(nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is empty", func() {
            node := makeNode("nodeA")
            ss, err := node.Get([][]byte{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains a nil key", func() {
            node := makeNode("nodeA")
            ss, err := node.Get([][]byte{ nil })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains an empty key", func() {
            node := makeNode("nodeA")
            ss, err := node.Get([][]byte{ []byte{ } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if the keys slice contains a key that is too long", func() {
            longKey := [MAX_SORTING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA")
            ss, err := node.Get([][]byte{ longKey[:] })
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if a storage driver error occurs", func() {
            node := makeNode("nodeA")
            ss, err := node.Get([][]byte{ []byte{ 0 } })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if the data is misformatted", func() {
            encodePartitionDataKey := func(k []byte) []byte {
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, k...)
                
                return result
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            
            node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey([]byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            ss, err := node.Get([][]byte{ []byte("sorting1") })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
    })
    
    Describe("#GetMatches", func() {
        It("should return EEmpty if the keys slice is nil", func() {
            node := makeNode("nodeA")
            ss, err := node.GetMatches(nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is empty", func() {
            node := makeNode("nodeA")
            ss, err := node.GetMatches([][]byte{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains a nil key", func() {
            node := makeNode("nodeA")
            ss, err := node.GetMatches([][]byte{ nil })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains an empty key", func() {
            node := makeNode("nodeA")
            ss, err := node.GetMatches([][]byte{ []byte{ } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if the keys slice contains a key that is too long", func() {
            longKey := [MAX_SORTING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA")
            ss, err := node.GetMatches([][]byte{ longKey[:] })
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if a storage driver error occurs", func() {
            node := makeNode("nodeA")
            ss, err := node.GetMatches([][]byte{ []byte{ 0 } })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if the data is misformatted", func() {
            encodePartitionDataKey := func(k []byte) []byte {
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, k...)
                
                return result
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            
            node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey([]byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            ss, err := node.GetMatches([][]byte{ []byte("sorting1") })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err).Should(BeNil())
            Expect(ss).Should(Not(BeNil()))
            
            Expect(ss.Next()).Should(BeFalse())
            Expect(ss.Key()).Should(BeNil())
            Expect(ss.Error().(DBerror).Code()).Should(Equal(EStorage.Code()))
        })
    })
    
    Describe("#Batch", func() {
        It("should return EEmpty if the batch is nil", func() {
            node := makeNode("nodeA")
            ss, err := node.Batch(nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if one of the sorting keys in an op is too long", func() {
            key := [MAX_SORTING_KEY_LENGTH+1]byte{ }
            updateBatch := NewUpdateBatch()
            _, err := updateBatch.Put(key[:], []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
        })
        
        It("should return EEmpty if one of the sorting keys is empty", func() {
            key := []byte{ }
            updateBatch := NewUpdateBatch()
            _, err := updateBatch.Put(key[:], []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
        })
        
        It("should return EStorage if the driver encounters an error", func() {
            key := []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        
            node := makeNode("nodeA")
            ss, err := node.Batch(updateBatch)
        
            // storage driver should be closed at this point
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if one of the ops affects a key that is already in the database but is corrupt or misformatted", func() {
            encodePartitionDataKey := func(k []byte) []byte {
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, k...)
                
                return result
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            
            node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey([]byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            updateBatch := NewUpdateBatch()
            updateBatch.Put([]byte("sorting1"), []byte("value123"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            ss, err := node.Batch(updateBatch)
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
    })
    
    Describe("#Merge", func() {
        It("should return EEmpty if the sibling sets map is nil", func() {
            node := makeNode("nodeA")
            err := node.Merge(nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
        })
    })
    
    Context("a key does not exist in the node", func() {
        var (
            storageEngine StorageDriver
            node *Node
        )
        
        BeforeEach(func() {
            storageEngine = makeNewStorageDriver()
            storageEngine.Open()
            
            node, _ = NewNode("nodeA", storageEngine, MerkleMinDepth, nil)
        })
        
        AfterEach(func() {
            storageEngine.Close()
        })
        
        It("should report nil as the value at that key when queried", func() {
            key := []byte(randomString())
            values, err := node.Get([][]byte{ key })
        
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(BeNil())
            Expect(err).Should(BeNil())
        })
        
        It("should not iterate over that key when queried with an iterator", func() {
            key := []byte(randomString())
            iter, err := node.GetMatches([][]byte{ key })
        
            Expect(iter).Should(Not(BeNil()))
            Expect(err).Should(BeNil())
            Expect(iter.Next()).Should(BeFalse())
        })    
    })
    
    Context("a key exists in the node", func() {
        var (
            storageEngine StorageDriver
            node *Node
            key []byte
        )
        
        BeforeEach(func() {
            storageEngine = makeNewStorageDriver()
            storageEngine.Open()
            
            node, _ = NewNode("nodeA", storageEngine, MerkleMinDepth, nil)
            
            key = []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            node.Batch(updateBatch)
        })
        
        AfterEach(func() {
            storageEngine.Close()
        })
        
        It("should contain a sibling set for that key when queried", func() {
            values, err := node.Get([][]byte{ key })
        
            Expect(err).Should(BeNil())
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(Not(BeNil()))
            Expect(values[0].Size()).Should(Equal(1))
        
            for sibling := range values[0].Iter() {
                Expect(string(sibling.Value())).Should(Equal("hello"))
            }
            
            iter, err := node.GetMatches([][]byte{ key })
            
            Expect(err).Should(BeNil())
            Expect(iter.Next()).Should(BeTrue())
            Expect(iter.Key()).Should(Equal(key))
            
            siblingSet := iter.Value()
            
            Expect(siblingSet.Size()).Should(Equal(1))
            
            for sibling := range siblingSet.Iter() {
                Expect(string(sibling.Value())).Should(Equal("hello"))
            }
            
            Expect(iter.Next()).Should(BeFalse())
        })
        
        It("should remove all siblings from a sibling set when deleting that key with an empty context", func() {
            updateBatch := NewUpdateBatch()
            updateBatch.Delete(key, NewDVV(NewDot("", 0), map[string]uint64{ }))
            _, err := node.Batch(updateBatch)
            
            Expect(err).Should(BeNil())
            
            values, err := node.Get([][]byte{ key })
        
            Expect(err).Should(BeNil())
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(Not(BeNil()))
            Expect(values[0].Size()).Should(Equal(1))
            Expect(values[0].IsTombstoneSet()).Should(BeTrue())
        })
        
        It("should add new siblings to a sibling set when putting that key with a parallel context", func() {
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello1"), NewDVV(NewDot("", 0), map[string]uint64{ "nodeA": 0 }))
            _, err := node.Batch(updateBatch)
            
            Expect(err).Should(BeNil())
            
            values, err := node.Get([][]byte{ key })
        
            Expect(err).Should(BeNil())
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(Not(BeNil()))
            Expect(values[0].Size()).Should(Equal(2))
            
            for sibling := range values[0].Iter() {
                Expect(string(sibling.Value()) == "hello" || string(sibling.Value()) == "hello1").Should(BeTrue())
            }
        })
    })
})

func BenchmarkSingleThreadBatches(b *testing.B) {
    storageEngine := makeNewStorageDriver()
    storageEngine.Open()
    defer storageEngine.Close()

    node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)

    c := 0
    fmt.Println("start 1")
    for i := 0; i < b.N; i += 1 {
        c += 1
        key := []byte(randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        node.Batch(updateBatch)
    }
    
    fmt.Println("1", c)
}

func BenchmarkSerializedBatches(b *testing.B) {
    storageEngine := makeNewStorageDriver()
    storageEngine.Open()
    defer storageEngine.Close()

    node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)

    var wg sync.WaitGroup

    c := 0
    fmt.Println("start 2")
    for i := 0; i < b.N; i += 1 {
        c += 1
        key := []byte(randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        wg.Add(1)
        go func() {
            node.Batch(updateBatch)
            wg.Done()
        }()
    }
    
    wg.Wait()
    fmt.Println("2", c)
}

func BenchmarkParallelBatches(b *testing.B) {
    storageEngine := makeNewStorageDriver()
    storageEngine.Open()
    defer storageEngine.Close()

    node, _ := NewNode("nodeA", storageEngine, MerkleMinDepth, nil)

    var wg sync.WaitGroup
        
    c := 0
    fmt.Println("start 3")
    for i := 0; i < b.N; i += 1 {
        c += 1
        key := []byte(randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        wg.Add(1)
        go func() {
            node.Batch(updateBatch)
            wg.Done()
        }()
    }
    
    wg.Wait()
    fmt.Println("3", c)
}