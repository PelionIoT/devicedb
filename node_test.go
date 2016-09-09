package devicedb_test

import (
	. "devicedb"

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
    makeNode := func(nodeID string, hashStrategy func([]byte) uint64, tokenSetMap map[string]TokenSet) *Node {
        storageEngine := makeNewStorageDriver()
        storageEngine.Open()
        defer storageEngine.Close()
        hashRing, _ := NewHashRing(nodeID, 3, hashStrategy)
        ringState := NewRingState()
        
        if tokenSetMap == nil {
            tokenSetMap = map[string]TokenSet{ }
            tokenSetMap[nodeID] = TokenSet{ 1, map[uint64]bool{ 0: true } }
        }
        
        ringState.SetTokens(tokenSetMap)
        
        hashRing.Update(ringState)
        
        node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)
        
        return node
    }
        
    Describe("#Get", func() {
        It("should return EEmpty if the partitioning key is nil", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get(nil, [][]byte{ []byte{ 0 } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the partitioning key is an empty slice", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ }, [][]byte{ []byte{ 0 } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is nil", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is empty", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, [][]byte{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains a nil key", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, [][]byte{ nil })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains an empty key", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, [][]byte{ []byte{ } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if the keys slice contains a key that is too long", func() {
            longKey := [MAX_PARTITIONING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, [][]byte{ longKey[:] })
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if a storage driver error occurs", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Get([]byte{ 0 }, [][]byte{ []byte{ 0 } })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if the data is misformatted", func() {
            encodePartitionDataKey := func(vnode uint64, p []byte, k []byte) []byte {
                vnodeEncoding := make([]byte, 8)
                binary.BigEndian.PutUint64(vnodeEncoding, vnode)
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(vnodeEncoding) + len(p) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, vnodeEncoding...) 
                result = append(result, p...)
                result = append(result, k...)
                
                return result
            }
            
            hashStrategy := func(key []byte) uint64 {
                return 0
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            hashRing, _ := NewHashRing("nodeA", 3, hashStrategy)
            ringState := NewRingState()
            tokenSetMap := map[string]TokenSet{ }
            tokenSetMap["nodeA"] = TokenSet{ 1, map[uint64]bool{ 0: true } }
            
            ringState.SetTokens(tokenSetMap)
            hashRing.Update(ringState)
            
            node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey(0, []byte{ 89 }, []byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            ss, err := node.Get([]byte{ 89 }, [][]byte{ []byte("sorting1") })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ENoVNode if the key the node does not contain the vnode where that key goes", func() {
            sliceSize := uint64(1 << 62)
            hashStrategy := func(key []byte) uint64 {
                return map[string]uint64{
                    "key1": 0,
                    "key2": sliceSize,
                    "key3": sliceSize * 2,
                    "key4": sliceSize * 3,
                }[string(key)]
            }
            
            tokenSetA := map[uint64]bool{ }
            tokenSetB := map[uint64]bool{ }
            tokenSetC := map[uint64]bool{ }
            tokenSetD := map[uint64]bool{ }
            
            tokenSetA[0] = true
            tokenSetB[sliceSize] = true
            tokenSetC[sliceSize * 2] = true
            tokenSetD[sliceSize * 3] = true
            
            tokenSetMap := map[string]TokenSet{ 
                "nodeA": TokenSet{ 1, tokenSetA },
                "nodeB": TokenSet{ 1, tokenSetB },
                "nodeC": TokenSet{ 1, tokenSetC },
                "nodeD": TokenSet{ 1, tokenSetD },
            }
        
            nodeA := makeNode("nodeA", hashStrategy, tokenSetMap)
            nodeB := makeNode("nodeB", hashStrategy, tokenSetMap)
            nodeC := makeNode("nodeC", hashStrategy, tokenSetMap)
            nodeD := makeNode("nodeD", hashStrategy, tokenSetMap)
            
            ss, err := nodeA.Get([]byte("key2"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeB.Get([]byte("key3"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeC.Get([]byte("key4"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeD.Get([]byte("key1"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
        })    
    })
    
    Describe("#GetMatches", func() {
        It("should return EEmpty if the partitioning key is nil", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches(nil, [][]byte{ []byte{ 0 } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the partitioning key is an empty slice", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ }, [][]byte{ []byte{ 0 } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is nil", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice is empty", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, [][]byte{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains a nil key", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, [][]byte{ nil })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the keys slice contains an empty key", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, [][]byte{ []byte{ } })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if the keys slice contains a key that is too long", func() {
            longKey := [MAX_PARTITIONING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, [][]byte{ longKey[:] })
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if a storage driver error occurs", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.GetMatches([]byte{ 0 }, [][]byte{ []byte{ 0 } })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if the data is misformatted", func() {
            encodePartitionDataKey := func(vnode uint64, p []byte, k []byte) []byte {
                vnodeEncoding := make([]byte, 8)
                binary.BigEndian.PutUint64(vnodeEncoding, vnode)
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(vnodeEncoding) + len(p) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, vnodeEncoding...) 
                result = append(result, p...)
                result = append(result, k...)
                
                return result
            }
            
            hashStrategy := func(key []byte) uint64 {
                return 0
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            hashRing, _ := NewHashRing("nodeA", 3, hashStrategy)
            ringState := NewRingState()
            tokenSetMap := map[string]TokenSet{ }
            tokenSetMap["nodeA"] = TokenSet{ 1, map[uint64]bool{ 0: true } }
            
            ringState.SetTokens(tokenSetMap)
            hashRing.Update(ringState)
            
            node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey(0, []byte{ 89 }, []byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            ss, err := node.GetMatches([]byte{ 89 }, [][]byte{ []byte("sorting1") })
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err).Should(BeNil())
            Expect(ss).Should(Not(BeNil()))
            
            Expect(ss.Next()).Should(BeFalse())
            Expect(ss.Key()).Should(BeNil())
            Expect(ss.Error().(DBerror).Code()).Should(Equal(EStorage.Code()))
        })
        
        It("should return ENoVNode if the key the node does not contain the vnode where that key goes", func() {
            sliceSize := uint64(1 << 62)
            hashStrategy := func(key []byte) uint64 {
                return map[string]uint64{
                    "key1": 0,
                    "key2": sliceSize,
                    "key3": sliceSize * 2,
                    "key4": sliceSize * 3,
                }[string(key)]
            }
            
            tokenSetA := map[uint64]bool{ }
            tokenSetB := map[uint64]bool{ }
            tokenSetC := map[uint64]bool{ }
            tokenSetD := map[uint64]bool{ }
            
            tokenSetA[0] = true
            tokenSetB[sliceSize] = true
            tokenSetC[sliceSize * 2] = true
            tokenSetD[sliceSize * 3] = true
            
            tokenSetMap := map[string]TokenSet{ 
                "nodeA": TokenSet{ 1, tokenSetA },
                "nodeB": TokenSet{ 1, tokenSetB },
                "nodeC": TokenSet{ 1, tokenSetC },
                "nodeD": TokenSet{ 1, tokenSetD },
            }
        
            nodeA := makeNode("nodeA", hashStrategy, tokenSetMap)
            nodeB := makeNode("nodeB", hashStrategy, tokenSetMap)
            nodeC := makeNode("nodeC", hashStrategy, tokenSetMap)
            nodeD := makeNode("nodeD", hashStrategy, tokenSetMap)
            
            ss, err := nodeA.GetMatches([]byte("key2"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeB.GetMatches([]byte("key3"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeC.GetMatches([]byte("key4"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeD.GetMatches([]byte("key1"), [][]byte{ []byte("sortingKey") })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
        })
    })
    
    Describe("#Batch", func() {
        It("should return EEmpty if the partitioning key is nil", func() {
            key := []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Batch(nil, updateBatch)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the partitioning key is empty", func() {
            key := []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Batch([]byte{ }, updateBatch)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ELength if the partitioning key is too long", func() {
            key := []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        
            partitioningKey := [MAX_PARTITIONING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Batch(partitioningKey[:], updateBatch)
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EEmpty if the batch is nil", func() {
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Batch([]byte{ 0 }, nil)
            
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
        
            node := makeNode("nodeA", nil, nil)
            ss, err := node.Batch([]byte{ 0 }, updateBatch)
        
            // storage driver should be closed at this point
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return ENoVNode if the key the node does not contain the vnode where that key goes", func() {
            sliceSize := uint64(1 << 62)
            hashStrategy := func(key []byte) uint64 {
                return map[string]uint64{
                    "key1": 0,
                    "key2": sliceSize,
                    "key3": sliceSize * 2,
                    "key4": sliceSize * 3,
                }[string(key)]
            }
            
            tokenSetA := map[uint64]bool{ }
            tokenSetB := map[uint64]bool{ }
            tokenSetC := map[uint64]bool{ }
            tokenSetD := map[uint64]bool{ }
            
            tokenSetA[0] = true
            tokenSetB[sliceSize] = true
            tokenSetC[sliceSize * 2] = true
            tokenSetD[sliceSize * 3] = true
            
            tokenSetMap := map[string]TokenSet{ 
                "nodeA": TokenSet{ 1, tokenSetA },
                "nodeB": TokenSet{ 1, tokenSetB },
                "nodeC": TokenSet{ 1, tokenSetC },
                "nodeD": TokenSet{ 1, tokenSetD },
            }
        
            nodeA := makeNode("nodeA", hashStrategy, tokenSetMap)
            nodeB := makeNode("nodeB", hashStrategy, tokenSetMap)
            nodeC := makeNode("nodeC", hashStrategy, tokenSetMap)
            nodeD := makeNode("nodeD", hashStrategy, tokenSetMap)
            
            key := []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            ss, err := nodeA.Batch([]byte("key2"), updateBatch)
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeB.Batch([]byte("key3"), updateBatch)
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeC.Batch([]byte("key4"), updateBatch)
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
            
            ss, err = nodeD.Batch([]byte("key1"), updateBatch)
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            Expect(ss).Should(BeNil())
        })
        
        It("should return EStorage if one of the ops affects a key that is already in the database but is corrupt or misformatted", func() {
            encodePartitionDataKey := func(vnode uint64, p []byte, k []byte) []byte {
                vnodeEncoding := make([]byte, 8)
                binary.BigEndian.PutUint64(vnodeEncoding, vnode)
                result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(vnodeEncoding) + len(p) + len(k))
                
                result = append(result, PARTITION_DATA_PREFIX...)
                result = append(result, vnodeEncoding...) 
                result = append(result, p...)
                result = append(result, k...)
                
                return result
            }
            
            hashStrategy := func(key []byte) uint64 {
                return 0
            }
            
            storageEngine := makeNewStorageDriver()
            storageEngine.Open()
            defer storageEngine.Close()
            hashRing, _ := NewHashRing("nodeA", 3, hashStrategy)
            ringState := NewRingState()
            tokenSetMap := map[string]TokenSet{ }
            tokenSetMap["nodeA"] = TokenSet{ 1, map[uint64]bool{ 0: true } }
            
            ringState.SetTokens(tokenSetMap)
            hashRing.Update(ringState)
            
            node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)
            batch := NewBatch()
            batch.Put(encodePartitionDataKey(0, []byte{ 89 }, []byte("sorting1")), []byte("value123"))
            err := storageEngine.Batch(batch)
            
            Expect(err).Should(BeNil())
            
            
            updateBatch := NewUpdateBatch()
            updateBatch.Put([]byte("sorting1"), []byte("value123"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            
            ss, err := node.Batch([]byte{ 89 }, updateBatch)
            
            // will cause an error because the storage driver was closed at the end of makeNode()
            Expect(err.(DBerror).Code()).Should(Equal(EStorage.Code()))
            Expect(ss).Should(BeNil())
        })
    })
    
    Describe("#Merge", func() {
        It("should return EEmpty if the partitioning key is nil", func() {
            node := makeNode("nodeA", nil, nil)
            err := node.Merge(nil, map[string]*SiblingSet{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
        })
        
        It("should return EEmpty if the partitioning key is empty", func() {
            node := makeNode("nodeA", nil, nil)
            err := node.Merge([]byte{ }, map[string]*SiblingSet{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
        })
        
        It("should return ELength if the partitioning key is too long", func() {
            partitioningKey := [MAX_PARTITIONING_KEY_LENGTH+1]byte{ }
            node := makeNode("nodeA", nil, nil)
            err := node.Merge(partitioningKey[:], map[string]*SiblingSet{ })
            
            Expect(err.(DBerror).Code()).Should(Equal(ELength.Code()))
        })
        
        It("should return EEmpty if the sibling sets map is nil", func() {
            node := makeNode("nodeA", nil, nil)
            err := node.Merge([]byte{ 0 }, nil)
            
            Expect(err.(DBerror).Code()).Should(Equal(EEmpty.Code()))
        })
        
        It("should return ENoVNode if the key the node does not contain the vnode where that key goes", func() {
            sliceSize := uint64(1 << 62)
            hashStrategy := func(key []byte) uint64 {
                return map[string]uint64{
                    "key1": 0,
                    "key2": sliceSize,
                    "key3": sliceSize * 2,
                    "key4": sliceSize * 3,
                }[string(key)]
            }
            
            tokenSetA := map[uint64]bool{ }
            tokenSetB := map[uint64]bool{ }
            tokenSetC := map[uint64]bool{ }
            tokenSetD := map[uint64]bool{ }
            
            tokenSetA[0] = true
            tokenSetB[sliceSize] = true
            tokenSetC[sliceSize * 2] = true
            tokenSetD[sliceSize * 3] = true
            
            tokenSetMap := map[string]TokenSet{ 
                "nodeA": TokenSet{ 1, tokenSetA },
                "nodeB": TokenSet{ 1, tokenSetB },
                "nodeC": TokenSet{ 1, tokenSetC },
                "nodeD": TokenSet{ 1, tokenSetD },
            }
        
            nodeA := makeNode("nodeA", hashStrategy, tokenSetMap)
            nodeB := makeNode("nodeB", hashStrategy, tokenSetMap)
            nodeC := makeNode("nodeC", hashStrategy, tokenSetMap)
            nodeD := makeNode("nodeD", hashStrategy, tokenSetMap)
            
            err := nodeA.Merge([]byte("key2"), map[string]*SiblingSet{ })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            
            err = nodeB.Merge([]byte("key3"), map[string]*SiblingSet{ })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            
            err = nodeC.Merge([]byte("key4"), map[string]*SiblingSet{ })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
            
            err = nodeD.Merge([]byte("key1"), map[string]*SiblingSet{ })
            Expect(err.(DBerror).Code()).Should(Equal(ENoVNode.Code()))
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
            hashRing, _ := NewHashRing("nodeA", 3, nil)
            ringState := NewRingState()
            
            ringState.SetTokens(map[string]TokenSet{
                "nodeA": TokenSet{ 1, map[uint64]bool{ 0: true } },
            })
            
            hashRing.Update(ringState)
            
            node, _ = NewNode(storageEngine, hashRing, MerkleMinDepth)
        })
        
        AfterEach(func() {
            storageEngine.Close()
        })
        
        It("should report nil as the value at that key when queried", func() {
            key := []byte(randomString())
            values, err := node.Get([]byte("account1"), [][]byte{ key })
        
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(BeNil())
            Expect(err).Should(BeNil())
        })
        
        It("should not iterate over that key when queried with an iterator", func() {
            key := []byte(randomString())
            iter, err := node.GetMatches([]byte("account1"), [][]byte{ key })
        
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
            hashRing, _ := NewHashRing("nodeA", 3, nil)
            ringState := NewRingState()
            
            ringState.SetTokens(map[string]TokenSet{
                "nodeA": TokenSet{ 1, map[uint64]bool{ 0: true } },
            })
            
            hashRing.Update(ringState)
            
            node, _ = NewNode(storageEngine, hashRing, MerkleMinDepth)
            
            key = []byte(randomString())
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            node.Batch([]byte("account1"), updateBatch)
        })
        
        AfterEach(func() {
            storageEngine.Close()
        })
        
        It("should contain a sibling set for that key when queried", func() {
            values, err := node.Get([]byte("account1"), [][]byte{ key })
        
            Expect(err).Should(BeNil())
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(Not(BeNil()))
            Expect(values[0].Size()).Should(Equal(1))
        
            for sibling := range values[0].Iter() {
                Expect(string(sibling.Value())).Should(Equal("hello"))
            }
            
            iter, err := node.GetMatches([]byte("account1"), [][]byte{ key })
            
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
            _, err := node.Batch([]byte("account1"), updateBatch)
            
            Expect(err).Should(BeNil())
            
            values, err := node.Get([]byte("account1"), [][]byte{ key })
        
            Expect(err).Should(BeNil())
            Expect(len(values)).Should(Equal(1))
            Expect(values[0]).Should(Not(BeNil()))
            Expect(values[0].Size()).Should(Equal(1))
            Expect(values[0].IsTombstoneSet()).Should(BeTrue())
        })
        
        It("should add new siblings to a sibling set when putting that key with a parallel context", func() {
            updateBatch := NewUpdateBatch()
            updateBatch.Put(key, []byte("hello1"), NewDVV(NewDot("", 0), map[string]uint64{ "nodeA": 0 }))
            _, err := node.Batch([]byte("account1"), updateBatch)
            
            Expect(err).Should(BeNil())
            
            values, err := node.Get([]byte("account1"), [][]byte{ key })
        
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
    hashRing, _ := NewHashRing("nodeA", 3, nil)
    ringState := NewRingState()

    ringState.SetTokens(map[string]TokenSet{
        "nodeA": TokenSet{ 1, map[uint64]bool{ 0: true } },
    })
        
    hashRing.Update(ringState)

    node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)

    c := 0
    fmt.Println("start 1")
    for i := 0; i < b.N; i += 1 {
        c += 1
        key := []byte(randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        node.Batch([]byte("account1"), updateBatch)
    }
    
    fmt.Println("1", c)
}

func BenchmarkSerializedBatches(b *testing.B) {
    storageEngine := makeNewStorageDriver()
    storageEngine.Open()
    defer storageEngine.Close()
    hashRing, _ := NewHashRing("nodeA", 3, nil)
    ringState := NewRingState()

    ringState.SetTokens(map[string]TokenSet{
        "nodeA": TokenSet{ 1, map[uint64]bool{ 0: true } },
    })
        
    hashRing.Update(ringState)

    node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)

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
            node.Batch([]byte("account1"), updateBatch)
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
    hashRing, _ := NewHashRing("nodeA", 3, nil)
    ringState := NewRingState()

    ringState.SetTokens(map[string]TokenSet{
        "nodeA": TokenSet{ 1, map[uint64]bool{ 0: true } },
    })
        
    hashRing.Update(ringState)

    node, _ := NewNode(storageEngine, hashRing, MerkleMinDepth)

    var wg sync.WaitGroup
        
    c := 0
    fmt.Println("start 3")
    for i := 0; i < b.N; i += 1 {
        c += 1
        partitioningKey := []byte(randomString())
        key := []byte(randomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte("hello"), NewDVV(NewDot("", 0), map[string]uint64{ }))
        wg.Add(1)
        go func() {
            node.Batch(partitioningKey, updateBatch)
            wg.Done()
        }()
    }
    
    wg.Wait()
    fmt.Println("3", c)
}