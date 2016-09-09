package halodb

import (
    "encoding/binary"
    "time"
    "errors"
)

const MAX_PARTITIONING_KEY_LENGTH = 255
const MAX_SORTING_KEY_LENGTH = 255
const COMPOUND_KEY_PREFIX_LENGTH_BYTES = 1

var MASTER_MERKLE_TREE_PREFIX = []byte{ 0 }
var PARTITION_MERKLE_TREE_PREFIX = []byte{ 1 }
var PARTITION_MERKLE_LEAF_PREFIX = []byte{ 2 }
var PARTITION_DATA_PREFIX = []byte{ 3 }

func vnodeBytes(vnode uint64) []byte {
    bytes := make([]byte, 8)
    
    binary.BigEndian.PutUint64(bytes, vnode)
    
    return bytes
}

func nodeBytes(node uint32) []byte {
    bytes := make([]byte, 4)
    
    binary.BigEndian.PutUint32(bytes, node)
    
    return bytes
}

func encodeMerkleLeafKey(vnode uint64, nodeID uint32) []byte {
    nodeIDEncoding := nodeBytes(nodeID)
    vnodeEncoding := vnodeBytes(vnode)
    result := make([]byte, 0, len(MASTER_MERKLE_TREE_PREFIX) + len(vnodeEncoding) + len(nodeIDEncoding))
    
    result = append(result, MASTER_MERKLE_TREE_PREFIX...)
    result = append(result, vnodeEncoding...)
    result = append(result, nodeIDEncoding...)
    
    return result
}

func decodeMerkleLeafKey(k []byte) (uint64, uint32, []byte, error) {
    k = k[len(MASTER_MERKLE_TREE_PREFIX):]
    
    if len(k) <= 12 {
        return 0, 0, nil, errors.New("Invalid merkle leaf key")
    }
    
    return binary.BigEndian.Uint64(k[:8]), binary.BigEndian.Uint32(k[8:12]), k[12:], nil
}

func encodePartitionMerkleNodeKey(vnode uint64, p []byte, nodeID uint32) []byte {
    nodeIDEncoding := nodeBytes(nodeID)
    vnodeEncoding := vnodeBytes(vnode)
    result := make([]byte, 0, len(PARTITION_MERKLE_TREE_PREFIX) + len(vnodeEncoding) + len(p) + len(nodeIDEncoding))
    
    result = append(result, PARTITION_MERKLE_TREE_PREFIX...)
    result = append(result, vnodeEncoding...)
    result = append(result, p...)
    result = append(result, nodeIDEncoding...)
    
    return result
}

func encodePartitionMerkleLeafKey(vnode uint64, nodeID uint32, p []byte, k []byte) []byte {
    nodeIDEncoding := nodeBytes(nodeID)
    vnodeEncoding := vnodeBytes(vnode)
    result := make([]byte, 0, len(PARTITION_MERKLE_TREE_PREFIX) + len(vnodeEncoding) + len(p) + len(nodeIDEncoding) + len(k))
    
    result = append(result, PARTITION_MERKLE_LEAF_PREFIX...)
    result = append(result, vnodeEncoding...)
    result = append(result, nodeIDEncoding...)
    result = append(result, p...)
    result = append(result, k...)
    
    return result
}

func encodePartitionDataKey(vnode uint64, p []byte, k []byte) []byte {
    vnodeEncoding := vnodeBytes(vnode)
    result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(vnodeEncoding) + len(p) + len(k))
    
    result = append(result, PARTITION_DATA_PREFIX...)
    result = append(result, vnodeEncoding...) 
    result = append(result, p...)
    result = append(result, k...)
    
    return result
}

func decodePartitionDataKey(p []byte, k []byte) []byte {
    return k[len(PARTITION_DATA_PREFIX) + 8 + len(p):]
}

type Node struct {
    storageDriver StorageDriver
    hashRing *HashRing
    vnodes map[uint64]*MerkleTree
    multiLock *MultiLock
}

func NewNode(storageDriver StorageDriver, hashRing *HashRing, merkleDepth uint8) (*Node, error) {
    node := Node{ storageDriver, hashRing, make(map[uint64]*MerkleTree), NewMultiLock() }
    
    for _, vnode := range hashRing.MyPartitions() {
        node.vnodes[vnode], _ = NewMerkleTree(merkleDepth)
    }
    
    err := node.initializeMerkleTrees()
        
    if err != nil {
        return nil, err
    }
    
    return &node, nil
}

func (node *Node) initializeMerkleTrees() error {
    iter, err := node.storageDriver.GetMatches([][]byte{ MASTER_MERKLE_TREE_PREFIX })
    
    if err != nil {
        return err
    }
    
    defer iter.Release()
    
    for iter.Next() {
        key := iter.Key()
        value := iter.Value()
        vnode, nodeID, _, err := decodeMerkleLeafKey(key)
        
        if err != nil {
            return err
        }
        
        if !node.ContainsVNode(vnode) {
            continue
        }
        
        if !node.vnodes[vnode].IsLeaf(nodeID) {
            return errors.New("Invalid leaf node in master merkle keys")
        }
        
        hashLow := value[:8]
        hashHigh := value[8:]
        hash := Hash{ }
        hash.SetHigh(binary.BigEndian.Uint64(hashHigh))
        hash.SetLow(binary.BigEndian.Uint64(hashLow))
    
        node.vnodes[vnode].UpdateLeafHash(nodeID, hash)
    }
    
    if iter.Error() != nil {
        return iter.Error()
    }
    
    return nil
}

func (node *Node) ContainsVNode(vnode uint64) bool {
    _, ok := node.vnodes[vnode]
    
    return ok
}

func (node *Node) ID() string {
    return node.hashRing.NodeID()
}

func (node *Node) Get(partitioningKey []byte, keys [][]byte) ([]*SiblingSet, error) {
    if len(partitioningKey) == 0 {
        log.Warningf("Passed empty partitioningKey parameter in Get(%v, %v)", partitioningKey, keys)
        
        return nil, EEmpty
    }
    
    if len(partitioningKey) > MAX_PARTITIONING_KEY_LENGTH {
        log.Warningf("Partitioning key is too long %d > %d in Get(%v, %v)", len(partitioningKey), MAX_PARTITIONING_KEY_LENGTH, partitioningKey, keys)
        
        return nil, ELength
    }
    
    if len(keys) == 0 {
        log.Warningf("Passed empty keys parameter in Get(%v, %v)", partitioningKey, keys)
        
        return nil, EEmpty
    }

    vnode := node.hashRing.PartitionNumber(partitioningKey)
    
    if !node.ContainsVNode(vnode) {
        log.Warningf("Tried to access vnode %v in Get(%v, %v). This node is not responsible for that vnode", vnode, partitioningKey, keys)
        
        return nil, ENoVNode
    }
    
    for i := 0; i < len(keys); i += 1 {
        if len(keys[i]) == 0 {
            log.Warningf("Passed empty key in Get(%v, %v)", partitioningKey, keys)
            
            return nil, EEmpty
        }
        
        if len(keys[i]) > MAX_SORTING_KEY_LENGTH {
            log.Warningf("Key is too long %d > %d in Get(%v, %v)", len(keys[i]), MAX_SORTING_KEY_LENGTH, partitioningKey, keys)
            
            return nil, ELength
        }
        
        keys[i] = encodePartitionDataKey(vnode, partitioningKey, keys[i])
    }
    
    // use storage driver
    values, err := node.storageDriver.Get(keys)
    
    if err != nil {
        log.Errorf("Storage driver error in Get(%v, %v): %s", partitioningKey, keys, err.Error())
        
        return nil, EStorage
    }
    
    siblingSetList := make([]*SiblingSet, len(keys))
    
    for i := 0; i < len(keys); i += 1 {
        if values[i] == nil {
            siblingSetList[i] = nil
            
            continue
        }
        
        var siblingSet SiblingSet
        
        err := siblingSet.Decode(values[i])
        
        if err != nil {
            log.Errorf("Storage driver error in Get(%v, %v): %s", partitioningKey, keys, err.Error())
            
            return nil, EStorage
        }
        
        siblingSetList[i] = &siblingSet
    }
    
    return siblingSetList, nil
}

func (node *Node) GetMatches(partitioningKey []byte, keys [][]byte) (*SiblingSetIterator, error) {
    if len(partitioningKey) == 0 {
        log.Warningf("Passed empty partitioningKey parameter in GetMatches(%v, %v)", partitioningKey, keys)
        
        return nil, EEmpty
    }
    
    if len(partitioningKey) > MAX_PARTITIONING_KEY_LENGTH {
        log.Warningf("Partitioning key is too long %d > %d in GetMatches(%v, %v)", len(partitioningKey), MAX_PARTITIONING_KEY_LENGTH, partitioningKey, keys)
        
        return nil, ELength
    }
    
    if len(keys) == 0 {
        log.Warningf("Passed empty keys parameter in GetMatches(%v, %v)", partitioningKey, keys)
        
        return nil, EEmpty
    }
    
    vnode := node.hashRing.PartitionNumber(partitioningKey)
    
    if !node.ContainsVNode(vnode) {
        log.Warningf("Tried to access vnode %v in GetMatches(%v, %v). This node is not responsible for that vnode", vnode, partitioningKey, keys)
        
        return nil, ENoVNode
    }
    
    for i := 0; i < len(keys); i += 1 {
        if len(keys[i]) == 0 {
            log.Warningf("Passed empty key in GetMatches(%v, %v)", partitioningKey, keys)
            
            return nil, EEmpty
        }
        
        if len(keys[i]) > MAX_SORTING_KEY_LENGTH {
            log.Warningf("Key is too long %d > %d in GetMatches(%v, %v)", len(keys[i]), MAX_SORTING_KEY_LENGTH, partitioningKey, keys)
            
            return nil, ELength
        }
        
        keys[i] = encodePartitionDataKey(vnode, partitioningKey, keys[i])
    }
    
    iter, err := node.storageDriver.GetMatches(keys)
    
    if err != nil {
        log.Errorf("Storage driver error in GetMatches(%v, %v): %s", partitioningKey, keys, err.Error())
            
        return nil, EStorage
    }
    
    return NewSiblingSetIterator(iter, partitioningKey), nil
}

func (node *Node) updateInit(vnode uint64, partitioningKey []byte, keys [][]byte) (map[string]*SiblingSet, error) {
    siblingSetMap := map[string]*SiblingSet{ }
    
    // db objects
    for i := 0; i < len(keys); i += 1 {
        keys[i] = encodePartitionDataKey(vnode, partitioningKey, keys[i])
    }
    
    values, err := node.storageDriver.Get(keys)
    
    if err != nil {
        log.Errorf("Storage driver error in updateInit(%v, %v, %v): %s", vnode, partitioningKey, keys, err.Error())
            
        return nil, EStorage
    }
    
    for i := 0; i < len(keys); i += 1 {
        var siblingSet SiblingSet
        key := decodePartitionDataKey(partitioningKey, keys[i])
        siblingSetBytes := values[0]
        
        if siblingSetBytes == nil {
            siblingSetMap[string(key)] = NewSiblingSet(map[*Sibling]bool{ })
        } else {
            err := siblingSet.Decode(siblingSetBytes)
            
            if err != nil {
                log.Warningf("Could not decode sibling set in updateInit(%v, %v, %v): %s", vnode, partitioningKey, keys, err.Error())
                
                return nil, EStorage
            }
            
            siblingSetMap[string(key)] = &siblingSet
        }
        
        values = values[1:]
    }
    
    return siblingSetMap, nil
}

func (node *Node) batch(vnode uint64, partitioningKey []byte, update *Update, merkleTree *MerkleTree) *Batch {
    _, leafNodes := merkleTree.Update(update)
    batch := NewBatch()
    
    // WRITE PARTITION MERKLE LEAFS
    for leafID, _ := range leafNodes {
        leafHash := merkleTree.NodeHash(leafID).Bytes()
        batch.Put(encodeMerkleLeafKey(vnode, leafID), leafHash[:])
        
        for key, _:= range leafNodes[leafID] {
            batch.Put(encodePartitionMerkleLeafKey(vnode, leafID, partitioningKey, []byte(key)), []byte{ })
        }
    }
    
    // WRITE PARTITION OBJECTS
    for diff := range update.Iter() {
        key := []byte(diff.Key())
        siblingSet := diff.NewSiblingSet()
        
        batch.Put(encodePartitionDataKey(vnode, partitioningKey, key), siblingSet.Encode())
    }

    return batch
}

func (node *Node) updateToSibling(o op, c *DVV, oldestTombstone *Sibling) *Sibling {
    if o.IsDelete() {
        if oldestTombstone == nil {
            return NewSibling(c, nil, uint64(time.Now().Unix()))
        } else {
            return NewSibling(c, nil, oldestTombstone.Timestamp())
        }
    } else {
        return NewSibling(c, o.Value(), uint64(time.Now().Unix()))
    }
}

func (node *Node) Batch(partitioningKey []byte, batch *UpdateBatch) (map[string]*SiblingSet, error) {
    if len(partitioningKey) == 0 {
        log.Warningf("Passed empty partitioningKey parameter in Batch(%v, %v)", partitioningKey, batch)
        
        return nil, EEmpty
    }
    
    if len(partitioningKey) > MAX_PARTITIONING_KEY_LENGTH {
        log.Warningf("Partitioning key is too long %d > %d in Batch(%v, %v)", len(partitioningKey), MAX_PARTITIONING_KEY_LENGTH, partitioningKey, batch)
        
        return nil, ELength
    }
    
    if batch == nil {
        log.Warningf("Passed nil batch parameter in Batch(%v, %v)", partitioningKey, batch)
        
        return nil, EEmpty
    }
    
    vnode := node.hashRing.PartitionNumber(partitioningKey)
    
    if !node.ContainsVNode(vnode) {
        log.Warningf("Tried to access vnode %v in Batch(%v, %v). This node is not responsible for that vnode", vnode, partitioningKey, batch)
        
        return nil, ENoVNode
    }
    
    keys := make([][]byte, 0, len(batch.batch.Ops()))
    update := NewUpdate()

    for key, _ := range batch.batch.Ops() {
        keyBytes := []byte(key)
        
        keys = append(keys, keyBytes)
    }

    merkleTree := node.vnodes[vnode]
    siblingSets, err := node.updateInit(vnode, partitioningKey, keys)
    
    if err != nil {
        return nil, err
    }
    
    for key, op := range batch.batch.Ops() {
        context := batch.context[key]
        siblingSet := siblingSets[key]
    
        if siblingSet.Size() == 0 && op.IsDelete() {
            delete(siblingSets, key)
            
            continue
        }
        
        updateContext := context.Context()
        
        if len(updateContext) == 0 {
            updateContext = siblingSet.Join()
        }
        
        updateClock := siblingSet.Event(updateContext, node.ID())
        var newSibling *Sibling
        
        if siblingSet.IsTombstoneSet() {
            newSibling = node.updateToSibling(op, updateClock, siblingSet.GetOldestTombstone())
        } else {
            newSibling = node.updateToSibling(op, updateClock, nil)
        }
        
        updatedSiblingSet := siblingSet.Discard(updateClock).Sync(NewSiblingSet(map[*Sibling]bool{ newSibling: true }))
    
        siblingSets[key] = updatedSiblingSet
        
        update.AddDiff(key, siblingSet, updatedSiblingSet)
    }
    
    err = node.storageDriver.Batch(node.batch(vnode, partitioningKey, update, merkleTree))
    
    if err != nil {
        log.Errorf("Storage driver error in Batch(%v, %v): %s", partitioningKey, batch, err.Error())
            
        return nil, EStorage
    }
    
    return siblingSets, nil
}

func (node *Node) Merge(partitioningKey []byte, siblingSets map[string]*SiblingSet) error {
    if len(partitioningKey) == 0 {
        log.Warningf("Passed nil partitioningKey parameter in Merge(%v, %v)", partitioningKey, siblingSets)
        
        return EEmpty
    }
    
    if len(partitioningKey) > MAX_PARTITIONING_KEY_LENGTH {
        log.Warningf("Partitioning key is too long %d > %d in Merge(%v, %v)", len(partitioningKey), MAX_PARTITIONING_KEY_LENGTH, partitioningKey, siblingSets)
        
        return ELength
    }
    
    if siblingSets == nil {
        log.Warningf("Passed nil sibling sets in Merge(%v, %v)", partitioningKey, siblingSets)
        
        return EEmpty
    }
    
    vnode := node.hashRing.PartitionNumber(partitioningKey)
    
    if !node.ContainsVNode(vnode) {
        log.Warningf("Tried to access vnode %v in Merge(%v, %v). This node is not responsible for that vnode", vnode, partitioningKey, siblingSets)
        
        return ENoVNode
    }

    keys := make([][]byte, 0, len(siblingSets))
    
    for key, _ := range siblingSets {
        keys = append(keys, []byte(key))
    }
    
    merkleTree := node.vnodes[vnode]
    siblingSets, err := node.updateInit(vnode, partitioningKey, keys)
    
    if err != nil {
        return err
    }
    
    update := NewUpdate()
        
    for _, key := range keys {
        siblingSet := siblingSets[string(key)]
        mySiblingSet := siblingSets[string(key)]
        updatedSiblingSet := siblingSet.Sync(mySiblingSet)
        
        for sibling := range updatedSiblingSet.Iter() {
            if !mySiblingSet.Has(sibling) {
                update.AddDiff(string(key), mySiblingSet, updatedSiblingSet)
            }
        }
    }
    
    if update.Size() != 0 {
        return node.storageDriver.Batch(node.batch(vnode, partitioningKey, update, merkleTree))
    }
    
    return nil
}

type UpdateBatch struct {
    batch *Batch
    context map[string]*DVV
}

func NewUpdateBatch() *UpdateBatch {
    return &UpdateBatch{ NewBatch(), map[string]*DVV{ } }
}

func (updateBatch *UpdateBatch) Put(key []byte, value []byte, context *DVV) (*UpdateBatch, error) {
    if len(key) == 0 {
        log.Warningf("Passed an empty key to Put(%v, %v, %v)", key, value, context)
        
        return nil, EEmpty
    }
    
    if len(key) > MAX_SORTING_KEY_LENGTH {
        log.Warningf("Key is too long %d > %d in Put(%v, %v, %v)", len(key), MAX_PARTITIONING_KEY_LENGTH, key, value, context)
        
        return nil, ELength
    }
    
    if context == nil {
        log.Warningf("Passed a nil context to Put(%v, %v, %v)", key, value, context)
        
        return nil, EEmpty
    }
    
    if value == nil {
        value = []byte{ }
    }
    
    updateBatch.batch.Put(key, value)
    updateBatch.context[string(key)] = context
    
    return updateBatch, nil
}

func (updateBatch *UpdateBatch) Delete(key []byte, context *DVV) (*UpdateBatch, error) {
    if len(key) == 0 {
        log.Warningf("Passed an empty key to Delete(%v, %v)", key, context)
        
        return nil, EEmpty
    }
    
    if len(key) > MAX_SORTING_KEY_LENGTH {
        log.Warningf("Key is too long %d > %d in Delete(%v, %v)", len(key), MAX_PARTITIONING_KEY_LENGTH, key, context)
        
        return nil, ELength
    }
    
    if context == nil {
        log.Warningf("Passed a nil context to Delete(%v, %v)", key, context)
        
        return nil, EEmpty
    }
    
    updateBatch.batch.Delete(key)
    updateBatch.context[string(key)] = context
    
    return updateBatch, nil
}

type SiblingSetIterator struct {
    dbIterator Iterator
    parseError error
    currentKey []byte
    currentValue *SiblingSet
    partitioningKey []byte
}

func NewSiblingSetIterator(dbIterator Iterator, partitioningKey []byte) *SiblingSetIterator {
    return &SiblingSetIterator{ dbIterator, nil, nil, nil, partitioningKey }
}

func (ssIterator *SiblingSetIterator) Next() bool {
    ssIterator.currentKey = nil
    ssIterator.currentValue = nil
    
    if !ssIterator.dbIterator.Next() {
        if ssIterator.dbIterator.Error() != nil {
            log.Errorf("Storage driver error in Next(): %s", ssIterator.dbIterator.Error())
        }
        
        return false
    }

    var siblingSet SiblingSet
    
    ssIterator.parseError = siblingSet.Decode(ssIterator.dbIterator.Value())
    
    if ssIterator.parseError != nil {
        log.Errorf("Storage driver error in Next() partitioningKey = %v, key = %v, value = %v: %s", ssIterator.partitioningKey, ssIterator.dbIterator.Key(), ssIterator.dbIterator.Value(), ssIterator.parseError.Error())
        
        ssIterator.Release()
        
        return false
    }
    
    ssIterator.currentKey = ssIterator.dbIterator.Key()
    ssIterator.currentValue = &siblingSet
    
    return true
}

func (ssIterator *SiblingSetIterator) Key() []byte {
    if ssIterator.currentKey == nil {
        return nil
    }
    
    return decodePartitionDataKey(ssIterator.partitioningKey, ssIterator.currentKey)
}

func (ssIterator *SiblingSetIterator) Value() *SiblingSet {
    return ssIterator.currentValue
}

func (ssIterator *SiblingSetIterator) Release() {
    ssIterator.dbIterator.Release()
}

func (ssIterator *SiblingSetIterator) Error() error {
    if ssIterator.parseError != nil {
        return EStorage
    }
        
    if ssIterator.dbIterator.Error() != nil {
        return EStorage
    }
    
    return nil
}

/*type Job struct {
}

type Dispatcher struct {
    jobs chan<- Job
    workerLimit int
    node *Node
    wg *sync.WaitGroup
}

func NewDispatcher(workerLimit int, node *Node) {
    return &Dispatcher{ make(chan<- Job), workerLimit, node, new(sync.WaitGroup) }
}

func (dispatcher *Dispatcher) Start() {
    for i := 0; i < dispatcher.workerLimit; i += 1 {
        dispatcher.wg.Add(1)
        
        go (func() {
            for job := range dispatcher.jobs {
                
            }
            
            dispatcher.wg.Done()
        })()
    }
}

func (dispatcher *Dispatcher) Stop() {
    close(dispatcher.jobs)
    
    dispatcher.wg.Wait()
}

func (dispatcher *Dispatcher) Batch(partitioningKey []byte, updateBatch *UpdateBatch) (map[string]*SiblingSet, error) {
}

func (dispatcher *Dispatcher) Merge(partitioningKey []byte, siblingSets map[string]*SiblingSet) error {
}*/

