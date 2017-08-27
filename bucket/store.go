
package bucket

import (
    "io"
    "encoding/json"
    "encoding/binary"
    "time"
    "errors"
    "sort"

    . "devicedb/storage"
    . "devicedb/error"
    . "devicedb/data"
    . "devicedb/logging"
    . "devicedb/merkle"
    . "devicedb/util"
    . "devicedb/resolver"
    . "devicedb/resolver/strategies"
)

const MAX_SORTING_KEY_LENGTH = 255

var MASTER_MERKLE_TREE_PREFIX = []byte{ 0 }
var PARTITION_MERKLE_LEAF_PREFIX = []byte{ 1 }
var PARTITION_DATA_PREFIX = []byte{ 2 }
var NODE_METADATA_PREFIX = []byte{ 3 }

func NanoToMilli(v uint64) uint64 {
    return v / 1000000
}

func nodeBytes(node uint32) []byte {
    bytes := make([]byte, 4)
    
    binary.BigEndian.PutUint32(bytes, node)
    
    return bytes
}

func encodeMerkleLeafKey(nodeID uint32) []byte {
    nodeIDEncoding := nodeBytes(nodeID)
    result := make([]byte, 0, len(MASTER_MERKLE_TREE_PREFIX) + len(nodeIDEncoding))
    
    result = append(result, MASTER_MERKLE_TREE_PREFIX...)
    result = append(result, nodeIDEncoding...)
    
    return result
}

func decodeMerkleLeafKey(k []byte) (uint32, error) {
    k = k[len(MASTER_MERKLE_TREE_PREFIX):]
    
    if len(k) != 4 {
        return 0, errors.New("Invalid merkle leaf key")
    }
    
    return binary.BigEndian.Uint32(k[:4]), nil
}

func encodePartitionDataKey(k []byte) []byte {
    result := make([]byte, 0, len(PARTITION_DATA_PREFIX) + len(k))
    
    result = append(result, PARTITION_DATA_PREFIX...)
    result = append(result, k...)
    
    return result
}

func decodePartitionDataKey(k []byte) []byte {
    return k[len(PARTITION_DATA_PREFIX):]
}

func encodePartitionMerkleLeafKey(nodeID uint32, k []byte) []byte {
    nodeIDEncoding := nodeBytes(nodeID)
    result := make([]byte, 0, len(PARTITION_MERKLE_LEAF_PREFIX) + len(nodeIDEncoding) + len(k))
    
    result = append(result, PARTITION_MERKLE_LEAF_PREFIX...)
    result = append(result, nodeIDEncoding...)
    result = append(result, k...)
    
    return result
}

func decodePartitionMerkleLeafKey(k []byte) (uint32, []byte, error) {
    if len(k) < len(PARTITION_MERKLE_LEAF_PREFIX) + 5 {
        return 0, nil, errors.New("Invalid partition merkle leaf key")
    }
    
    k = k[len(PARTITION_MERKLE_LEAF_PREFIX):]
    
    return binary.BigEndian.Uint32(k[:4]), k[4:], nil
}

func encodeMetadataKey(k []byte) []byte {
    result := make([]byte, 0, len(NODE_METADATA_PREFIX) + len(k))
    result = append(result, PARTITION_MERKLE_LEAF_PREFIX...)
    result = append(result, k...)
    
    return result
}

type Store struct {
    nodeID string
    storageDriver StorageDriver
    merkleTree *MerkleTree
    multiLock *MultiLock
    merkleLock *MultiLock
    conflictResolver ConflictResolver
}

func (store *Store) Initialize(nodeID string, storageDriver StorageDriver, merkleDepth uint8, conflictResolver ConflictResolver) error {
    if conflictResolver == nil {
        conflictResolver = &MultiValue{}
    }

    store.nodeID = nodeID
    store.storageDriver = storageDriver
    store.multiLock = NewMultiLock()
    store.merkleLock = NewMultiLock()
    store.conflictResolver = conflictResolver
    store.merkleTree, _ = NewMerkleTree(merkleDepth)
    
    var err error
    dbMerkleDepth, err := store.getStoreMetadata()
    
    if err != nil {
        Log.Errorf("Error retrieving database metadata for node %s: %v", nodeID, err)
        
        return err
    }
    
    if dbMerkleDepth != merkleDepth {
        Log.Debugf("Initializing node %s rebuilding merkle leafs with depth %d", nodeID, merkleDepth)
        
        err = store.RebuildMerkleLeafs()
        
        if err != nil {
            Log.Errorf("Error rebuilding merkle leafs for node %s: %v", nodeID, err)
            
            return err
        }
        
        err = store.RecordMetadata()
        
        if err != nil {
            Log.Errorf("Error recording merkle depth metadata for node %s: %v", nodeID, err)
            
            return err
        }
    }
    
    err = store.initializeMerkleTree()
    
    if err != nil {
        Log.Errorf("Error initializing node %s: %v", nodeID, err)
        
        return err
    }
    
    return nil
}

func (store *Store) initializeMerkleTree() error {
    iter, err := store.storageDriver.GetMatches([][]byte{ MASTER_MERKLE_TREE_PREFIX })
    
    if err != nil {
        return err
    }
    
    defer iter.Release()
    
    for iter.Next() {
        key := iter.Key()
        value := iter.Value()
        nodeID, err := decodeMerkleLeafKey(key)
            
        if err != nil {
            return err
        }
        
        if !store.merkleTree.IsLeaf(nodeID) {
            return errors.New("Invalid leaf node in master merkle keys")
        }
        
        hash := Hash{ }
        high := binary.BigEndian.Uint64(value[:8])
        low := binary.BigEndian.Uint64(value[8:])
        hash = hash.SetLow(low).SetHigh(high)
    
        store.merkleTree.UpdateLeafHash(nodeID, hash)
    }
    
    if iter.Error() != nil {
        return iter.Error()
    }
    
    return nil
}

func (store *Store) getStoreMetadata() (uint8, error) {
    values, err := store.storageDriver.Get([][]byte{ encodeMetadataKey([]byte("merkleDepth")) })
    
    if err != nil {
        return 0, err
    }
    
    if values[0] == nil || len(values[0]) == 0 {
        return 0, nil
    }
    
    return uint8(values[0][0]), nil
}

func (store *Store) RecordMetadata() error {
    batch := NewBatch()
    
    batch.Put(encodeMetadataKey([]byte("merkleDepth")), []byte{ byte(store.merkleTree.Depth()) })
    
    err := store.storageDriver.Batch(batch)
    
    if err != nil {
        return err
    }
    
    return nil
}

func (store *Store) RebuildMerkleLeafs() error {
    // Delete all keys starting with MASTER_MERKLE_TREE_PREFIX or PARTITION_MERKLE_LEAF_PREFIX
    iter, err := store.storageDriver.GetMatches([][]byte{ MASTER_MERKLE_TREE_PREFIX, PARTITION_MERKLE_LEAF_PREFIX })
    
    if err != nil {
        return err
    }
    
    defer iter.Release()
    
    for iter.Next() {
        batch := NewBatch()
        batch.Delete(iter.Key())
        err := store.storageDriver.Batch(batch)
        
        if err != nil {
            return err
        }
    }
    
    if iter.Error() != nil {
        return iter.Error()
    }
    
    iter.Release()

    // Scan through all the keys in this node and rebuild the merkle tree
    merkleTree, _ := NewMerkleTree(store.merkleTree.Depth())
    iter, err = store.storageDriver.GetMatches([][]byte{ PARTITION_DATA_PREFIX })
    
    if err != nil {
        return err
    }
    
    siblingSetIterator := NewBasicSiblingSetIterator(iter)
    
    defer siblingSetIterator.Release()
    
    for siblingSetIterator.Next() {
        key := siblingSetIterator.Key()
        siblingSet := siblingSetIterator.Value()
        update := NewUpdate().AddDiff(string(key), nil, siblingSet)
        
        _, leafNodes := merkleTree.Update(update)
        batch := NewBatch()
        
        for leafID, _ := range leafNodes {
            for key, _:= range leafNodes[leafID] {
                batch.Put(encodePartitionMerkleLeafKey(leafID, []byte(key)), []byte{ })
            }
        }
        
        err := store.storageDriver.Batch(batch)
        
        if err != nil {
            return err
        }
    }
    
    for leafID := uint32(1); leafID < merkleTree.NodeLimit(); leafID += 2 {
        batch := NewBatch()
        
        if merkleTree.NodeHash(leafID).High() != 0 || merkleTree.NodeHash(leafID).Low() != 0 {
            leafHash := merkleTree.NodeHash(leafID).Bytes()
            batch.Put(encodeMerkleLeafKey(leafID), leafHash[:])
            
            err := store.storageDriver.Batch(batch)
            
            if err != nil {
                return err
            }
        }
    }
    
    return siblingSetIterator.Error()
}

func (store *Store) MerkleTree() *MerkleTree {
    return store.merkleTree
}

func (store *Store) GarbageCollect(tombstonePurgeAge uint64) error {
    iter, err := store.storageDriver.GetMatches([][]byte{ PARTITION_DATA_PREFIX })
    
    if err != nil {
        Log.Errorf("Garbage collection error: %s", err.Error())
            
        return EStorage
    }
    
    now := NanoToMilli(uint64(time.Now().UnixNano()))
    siblingSetIterator := NewBasicSiblingSetIterator(iter)
    defer siblingSetIterator.Release()
    
    if tombstonePurgeAge > now {
        tombstonePurgeAge = now
    }
    
    for siblingSetIterator.Next() {
        var err error
        key := siblingSetIterator.Key()
        ssInitial := siblingSetIterator.Value()
        
        if !ssInitial.CanPurge(now - tombstonePurgeAge) {
            continue
        }
        
        store.lock([][]byte{ key })
        
        func() {
            // the key must be re-queried because at the time of iteration we did not have a lock
            // on the key in order to update it
            siblingSets, err := store.Get([][]byte{ key })
            
            if err != nil {
                return
            }
            
            siblingSet := siblingSets[0]
            
            if siblingSet == nil {
                return
            }
        
            if !siblingSet.CanPurge(now - tombstonePurgeAge) {
                return
            }
        
            Log.Debugf("GC: Purge tombstone at key %s. It is older than %d milliseconds", string(key), tombstonePurgeAge)
            leafID := store.merkleTree.LeafNode(key)
            
            batch := NewBatch()
            batch.Delete(encodePartitionMerkleLeafKey(leafID, key))
            batch.Delete(encodePartitionDataKey(key))
        
            err = store.storageDriver.Batch(batch)
        }()
        
        store.unlock([][]byte{ key }, false)
        
        if err != nil {
            Log.Errorf("Garbage collection error: %s", err.Error())
            
            return EStorage
        }
    }
    
    if iter.Error() != nil {
        Log.Errorf("Garbage collection error: %s", iter.Error().Error())
        
        return EStorage
    }
    
    return nil
}

func (store *Store) Get(keys [][]byte) ([]*SiblingSet, error) {
    if len(keys) == 0 {
        Log.Warningf("Passed empty keys parameter in Get(%v)", keys)
        
        return nil, EEmpty
    }

    for i := 0; i < len(keys); i += 1 {
        if len(keys[i]) == 0 {
            Log.Warningf("Passed empty key in Get(%v)", keys)
            
            return nil, EEmpty
        }
        
        if len(keys[i]) > MAX_SORTING_KEY_LENGTH {
            Log.Warningf("Key is too long %d > %d in Get(%v)", len(keys[i]), MAX_SORTING_KEY_LENGTH, keys)
            
            return nil, ELength
        }
        
        keys[i] = encodePartitionDataKey(keys[i])
    }
    
    // use storage driver
    values, err := store.storageDriver.Get(keys)
    
    if err != nil {
        Log.Errorf("Storage driver error in Get(%v): %s", keys, err.Error())
        
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
            Log.Errorf("Storage driver error in Get(%v): %s", keys, err.Error())
            
            return nil, EStorage
        }
        
        siblingSetList[i] = &siblingSet
    }
    
    return siblingSetList, nil
}

func (store *Store) GetMatches(keys [][]byte) (SiblingSetIterator, error) {
    if len(keys) == 0 {
        Log.Warningf("Passed empty keys parameter in GetMatches(%v)", keys)
        
        return nil, EEmpty
    }
    
    for i := 0; i < len(keys); i += 1 {
        if len(keys[i]) == 0 {
            Log.Warningf("Passed empty key in GetMatches(%v)", keys)
            
            return nil, EEmpty
        }
        
        if len(keys[i]) > MAX_SORTING_KEY_LENGTH {
            Log.Warningf("Key is too long %d > %d in GetMatches(%v)", len(keys[i]), MAX_SORTING_KEY_LENGTH, keys)
            
            return nil, ELength
        }
        
        keys[i] = encodePartitionDataKey(keys[i])
    }
    
    iter, err := store.storageDriver.GetMatches(keys)
    
    if err != nil {
        Log.Errorf("Storage driver error in GetMatches(%v): %s", keys, err.Error())
            
        return nil, EStorage
    }
    
    return NewBasicSiblingSetIterator(iter), nil
}

func (store *Store) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    if nodeID >= store.merkleTree.NodeLimit() {
        return nil, EMerkleRange
    }
    
    min := encodePartitionMerkleLeafKey(store.merkleTree.SubRangeMin(nodeID), []byte{})
    max := encodePartitionMerkleLeafKey(store.merkleTree.SubRangeMax(nodeID), []byte{})
    
    iter, err := store.storageDriver.GetRange(min, max)
    
    if err != nil {
        Log.Errorf("Storage driver error in GetSyncChildren(%v): %s", nodeID, err.Error())
        
        return nil, EStorage
    }

    return NewMerkleChildrenIterator(iter, store.storageDriver), nil
}

func (store *Store) Forget(keys [][]byte) error {
    for _, key := range keys {
        if key == nil {
            continue
        }

        store.lock([][]byte{ key })
        
        siblingSets, err := store.Get([][]byte{ key })
        
        if err != nil {
            Log.Errorf("Unable to forget key %s due to storage error: %v", string(key), err)

            store.unlock([][]byte{ key }, false)

            return EStorage
        }
        
        siblingSet := siblingSets[0]
        
        if siblingSet == nil {
            store.unlock([][]byte{ key }, false)

            continue
        }

        // Update merkle tree to reflect deletion
        leafID := store.merkleTree.LeafNode(key)
        newLeafHash := store.merkleTree.NodeHash(leafID).Xor(siblingSet.Hash(key))
        store.merkleTree.UpdateLeafHash(leafID, newLeafHash)

        batch := NewBatch()
        batch.Delete(encodePartitionMerkleLeafKey(leafID, key))
        batch.Delete(encodePartitionDataKey(key))
        leafHashBytes := newLeafHash.Bytes()
        batch.Put(encodeMerkleLeafKey(leafID), leafHashBytes[:])
    
        err = store.storageDriver.Batch(batch)
        
        store.unlock([][]byte{ key }, false)
        
        if err != nil {
            Log.Errorf("Unable to forget key %s due to storage error: %v", string(key), err.Error())
            
            return EStorage
        }

        Log.Debugf("Forgot key %s", string(key))
    }
    
    return nil
}

func (store *Store) updateInit(keys [][]byte) (map[string]*SiblingSet, error) {
    siblingSetMap := map[string]*SiblingSet{ }
    
    // db objects
    for i := 0; i < len(keys); i += 1 {
        keys[i] = encodePartitionDataKey(keys[i])
    }
    
    values, err := store.storageDriver.Get(keys)
    
    if err != nil {
        Log.Errorf("Storage driver error in updateInit(%v): %s", keys, err.Error())
        
        return nil, EStorage
    }
    
    for i := 0; i < len(keys); i += 1 {
        var siblingSet SiblingSet
        key := decodePartitionDataKey(keys[i])
        siblingSetBytes := values[0]
        
        if siblingSetBytes == nil {
            siblingSetMap[string(key)] = NewSiblingSet(map[*Sibling]bool{ })
        } else {
            err := siblingSet.Decode(siblingSetBytes)
            
            if err != nil {
                Log.Warningf("Could not decode sibling set in updateInit(%v): %s", keys, err.Error())
                
                return nil, EStorage
            }
            
            siblingSetMap[string(key)] = &siblingSet
        }
        
        values = values[1:]
    }
    
    return siblingSetMap, nil
}

func (store *Store) batch(update *Update, merkleTree *MerkleTree) *Batch {
    _, leafNodes := merkleTree.Update(update)
    batch := NewBatch()

    // WRITE PARTITION MERKLE LEAFS
    for leafID, _ := range leafNodes {
        leafHash := merkleTree.NodeHash(leafID).Bytes()
        batch.Put(encodeMerkleLeafKey(leafID), leafHash[:])
        
        for key, _ := range leafNodes[leafID] {
            batch.Put(encodePartitionMerkleLeafKey(leafID, []byte(key)), []byte{ })
        }
    }
    
    // WRITE PARTITION OBJECTS
    for diff := range update.Iter() {
        key := []byte(diff.Key())
        siblingSet := diff.NewSiblingSet()
        
        batch.Put(encodePartitionDataKey(key), siblingSet.Encode())
    }

    return batch
}

func (store *Store) updateToSibling(o Op, c *DVV, oldestTombstone *Sibling) *Sibling {
    if o.IsDelete() {
        if oldestTombstone == nil {
            return NewSibling(c, nil, NanoToMilli(uint64(time.Now().UnixNano())))
        } else {
            return NewSibling(c, nil, oldestTombstone.Timestamp())
        }
    } else {
        return NewSibling(c, o.Value(), NanoToMilli(uint64(time.Now().UnixNano())))
    }
}

func (store *Store) Batch(batch *UpdateBatch) (map[string]*SiblingSet, error) {
    if batch == nil {
        Log.Warningf("Passed nil batch parameter in Batch(%v)", batch)
        
        return nil, EEmpty
    }
        
    keys := make([][]byte, 0, len(batch.Batch().Ops()))
    update := NewUpdate()

    for key, _ := range batch.Batch().Ops() {
        keyBytes := []byte(key)
        
        keys = append(keys, keyBytes)
    }
    
    store.lock(keys)
    defer store.unlock(keys, true)

    merkleTree := store.merkleTree
    siblingSets, err := store.updateInit(keys)
    
    //return nil, nil
    if err != nil {
        return nil, err
    }
    
    for key, op := range batch.Batch().Ops() {
        context := batch.Context()[key]
        siblingSet := siblingSets[key]
    
        if siblingSet.Size() == 0 && op.IsDelete() {
            delete(siblingSets, key)
            
            continue
        }
        
        updateContext := context.Context()
        
        if len(updateContext) == 0 {
            updateContext = siblingSet.Join()
        }
        
        updateClock := siblingSet.Event(updateContext, store.nodeID)
        var newSibling *Sibling
        
        if siblingSet.IsTombstoneSet() {
            newSibling = store.updateToSibling(op, updateClock, siblingSet.GetOldestTombstone())
        } else {
            newSibling = store.updateToSibling(op, updateClock, nil)
        }
        
        updatedSiblingSet := siblingSet.Discard(updateClock).Sync(NewSiblingSet(map[*Sibling]bool{ newSibling: true }))
    
        siblingSets[key] = updatedSiblingSet
        
        updatedSiblingSet = store.conflictResolver.ResolveConflicts(updatedSiblingSet)
        
        update.AddDiff(key, siblingSet, updatedSiblingSet)
    }
    
    err = store.storageDriver.Batch(store.batch(update, merkleTree))
    
    if err != nil {
        Log.Errorf("Storage driver error in Batch(%v): %s", batch, err.Error())

        store.merkleTree.UndoUpdate(update)
        
        return nil, EStorage
    }
    
    return siblingSets, nil
}

func (store *Store) Merge(siblingSets map[string]*SiblingSet) error {
    if siblingSets == nil {
        Log.Warningf("Passed nil sibling sets in Merge(%v)", siblingSets)
        
        return EEmpty
    }
    
    keys := make([][]byte, 0, len(siblingSets))
    
    for key, _ := range siblingSets {
        keys = append(keys, []byte(key))
    }
    
    store.lock(keys)
    defer store.unlock(keys, true)
    
    merkleTree := store.merkleTree
    mySiblingSets, err := store.updateInit(keys)
    
    if err != nil {
        return err
    }
    
    update := NewUpdate()
        
    for _, key := range keys {
        key = decodePartitionDataKey(key)
        siblingSet := siblingSets[string(key)]
        mySiblingSet := mySiblingSets[string(key)]
        
        if siblingSet == nil {
            continue
        }
        
        if mySiblingSet == nil {
            mySiblingSet = NewSiblingSet(map[*Sibling]bool{ })
        }
        
        updatedSiblingSet := siblingSet.Sync(mySiblingSet)
        
        for sibling := range updatedSiblingSet.Iter() {
            if !mySiblingSet.Has(sibling) {
                updatedSiblingSet = store.conflictResolver.ResolveConflicts(updatedSiblingSet)
                
                update.AddDiff(string(key), mySiblingSet, updatedSiblingSet)
            }
        }
    }

    if update.Size() != 0 {
        err := store.storageDriver.Batch(store.batch(update, merkleTree))

        if err != nil {
            Log.Errorf("Storage driver error in Merge(%v): %s", siblingSets, err.Error())

            store.merkleTree.UndoUpdate(update)

            return EStorage
        }
    }
    
    return nil
}

func (store *Store) sortedLockKeys(keys [][]byte) ([]string, []string) {
    leafSet := make(map[string]bool, len(keys))
    keyStrings := make([]string, 0, len(keys))
    nodeStrings := make([]string, 0, len(keys))
    
    for _, key := range keys {
        keyStrings = append(keyStrings, string(key))
        leafSet[string(nodeBytes(store.merkleTree.LeafNode(key)))] = true
    }
    
    for node, _ := range leafSet {
        nodeStrings = append(nodeStrings, node)
    }
    
    sort.Strings(keyStrings)
    sort.Strings(nodeStrings)
    
    return keyStrings, nodeStrings
}

func (store *Store) lock(keys [][]byte) {
    keyStrings, nodeStrings := store.sortedLockKeys(keys)
    
    for _, key := range keyStrings {
        store.multiLock.Lock([]byte(key))
    }
    
    for _, key := range nodeStrings {
        store.merkleLock.Lock([]byte(key))
    }
}

func (store *Store) unlock(keys [][]byte, keysArePrefixed bool) {
    tKeys := make([][]byte, 0, len(keys))
    
    for _, key := range keys {
        if keysArePrefixed {
            tKeys = append(tKeys, key[1:])
        } else {
            tKeys = append(tKeys, key)
        }
    }
    
    keyStrings, nodeStrings := store.sortedLockKeys(tKeys)
    
    for _, key := range keyStrings {
        store.multiLock.Unlock([]byte(key))
    }
    
    for _, key := range nodeStrings {
        store.merkleLock.Unlock([]byte(key))
    }
}

type UpdateBatch struct {
    RawBatch *Batch `json:"batch"`
    Contexts map[string]*DVV `json:"context"`
}

func NewUpdateBatch() *UpdateBatch {
    return &UpdateBatch{ NewBatch(), map[string]*DVV{ } }
}

func (updateBatch *UpdateBatch) Batch() *Batch {
    return updateBatch.RawBatch
}

func (updateBatch *UpdateBatch) Context() map[string]*DVV {
    return updateBatch.Contexts
}

func (updateBatch *UpdateBatch) ToJSON() ([]byte, error) {
    return json.Marshal(updateBatch)
}

func (updateBatch *UpdateBatch) FromJSON(reader io.Reader) error {
    var tempUpdateBatch UpdateBatch
    
    decoder := json.NewDecoder(reader)
    err := decoder.Decode(&tempUpdateBatch)
    
    if err != nil {
        return err
    }
    
    updateBatch.Contexts = map[string]*DVV{ }
    updateBatch.RawBatch = NewBatch()
    
    for k, op := range tempUpdateBatch.Batch().Ops() {
        context, ok := tempUpdateBatch.Context()[k]
        
        if !ok || context == nil {
            context = NewDVV(NewDot("", 0), map[string]uint64{ })
        }
        
        err = nil
    
        if op.IsDelete() {
            _, err = updateBatch.Delete(op.Key(), context)
        } else {
            _, err = updateBatch.Put(op.Key(), op.Value(), context)
        }
        
        if err != nil {
            return err
        }
    }
    
    return nil
}

func (updateBatch *UpdateBatch) Put(key []byte, value []byte, context *DVV) (*UpdateBatch, error) {
    if len(key) == 0 {
        Log.Warningf("Passed an empty key to Put(%v, %v, %v)", key, value, context)
        
        return nil, EEmpty
    }
    
    if len(key) > MAX_SORTING_KEY_LENGTH {
        Log.Warningf("Key is too long %d > %d in Put(%v, %v, %v)", len(key), MAX_SORTING_KEY_LENGTH, key, value, context)
        
        return nil, ELength
    }
    
    if context == nil {
        Log.Warningf("Passed a nil context to Put(%v, %v, %v)", key, value, context)
        
        return nil, EEmpty
    }
    
    if value == nil {
        value = []byte{ }
    }
    
    updateBatch.Batch().Put(key, value)
    updateBatch.Context()[string(key)] = context
    
    return updateBatch, nil
}

func (updateBatch *UpdateBatch) Delete(key []byte, context *DVV) (*UpdateBatch, error) {
    if len(key) == 0 {
        Log.Warningf("Passed an empty key to Delete(%v, %v)", key, context)
        
        return nil, EEmpty
    }
    
    if len(key) > MAX_SORTING_KEY_LENGTH {
        Log.Warningf("Key is too long %d > %d in Delete(%v, %v)", len(key), MAX_SORTING_KEY_LENGTH, key, context)
        
        return nil, ELength
    }
    
    if context == nil {
        Log.Warningf("Passed a nil context to Delete(%v, %v)", key, context)
        
        return nil, EEmpty
    }
    
    updateBatch.Batch().Delete(key)
    updateBatch.Context()[string(key)] = context
    
    return updateBatch, nil
}

type MerkleChildrenIterator struct {
    dbIterator StorageIterator
    storageDriver StorageDriver
    parseError error
    currentKey []byte
    currentValue *SiblingSet
}

func NewMerkleChildrenIterator(iter StorageIterator, storageDriver StorageDriver) *MerkleChildrenIterator {
    return &MerkleChildrenIterator{ iter, storageDriver, nil, nil, nil }
    // not actually the prefix for all keys in the range, but it will be a consistent length
    // prefix := encodePartitionMerkleLeafKey(nodeID, []byte{ })
}

func (mIterator *MerkleChildrenIterator) Next() bool {
    mIterator.currentKey = nil
    mIterator.currentValue = nil
    
    if !mIterator.dbIterator.Next() {
        if mIterator.dbIterator.Error() != nil {
            Log.Errorf("Storage driver error in Next(): %s", mIterator.dbIterator.Error())
        }
        
        mIterator.Release()
        
        return false
    }
    
    _, key, err := decodePartitionMerkleLeafKey(mIterator.dbIterator.Key())
    
    if err != nil {
        Log.Errorf("Corrupt partition merkle leaf key in Next(): %v", mIterator.dbIterator.Key())
        
        mIterator.Release()
        
        return false
    }
    
    values, err := mIterator.storageDriver.Get([][]byte{ encodePartitionDataKey(key) })
    
    if err != nil {
        Log.Errorf("Storage driver error in Next(): %s", err)
        
        mIterator.Release()
        
        return false
    }
    
    value := values[0]

    var siblingSet SiblingSet
    
    mIterator.parseError = siblingSet.Decode(value)
    
    if mIterator.parseError != nil {
        Log.Errorf("Storage driver error in Next() key = %v, value = %v: %s", key, value, mIterator.parseError.Error())
        
        mIterator.Release()
        
        return false
    }
    
    mIterator.currentKey = key
    mIterator.currentValue = &siblingSet
    
    return true
}

func (mIterator *MerkleChildrenIterator) Prefix() []byte {
    return nil
}

func (mIterator *MerkleChildrenIterator) Key() []byte {
    return mIterator.currentKey
}

func (mIterator *MerkleChildrenIterator) Value() *SiblingSet {
    return mIterator.currentValue
}

func (mIterator *MerkleChildrenIterator) Release() {
    mIterator.dbIterator.Release()
}

func (mIterator *MerkleChildrenIterator) Error() error {
    if mIterator.parseError != nil {
        return EStorage
    }
        
    if mIterator.dbIterator.Error() != nil {
        return EStorage
    }
    
    return nil
}

type BasicSiblingSetIterator struct {
    dbIterator StorageIterator
    parseError error
    currentKey []byte
    currentValue *SiblingSet
}

func NewBasicSiblingSetIterator(dbIterator StorageIterator) *BasicSiblingSetIterator {
    return &BasicSiblingSetIterator{ dbIterator, nil, nil, nil }
}

func (ssIterator *BasicSiblingSetIterator) Next() bool {
    ssIterator.currentKey = nil
    ssIterator.currentValue = nil
    
    if !ssIterator.dbIterator.Next() {
        if ssIterator.dbIterator.Error() != nil {
            Log.Errorf("Storage driver error in Next(): %s", ssIterator.dbIterator.Error())
        }
        
        return false
    }

    var siblingSet SiblingSet
    
    ssIterator.parseError = siblingSet.Decode(ssIterator.dbIterator.Value())
    
    if ssIterator.parseError != nil {
        Log.Errorf("Storage driver error in Next() key = %v, value = %v: %s", ssIterator.dbIterator.Key(), ssIterator.dbIterator.Value(), ssIterator.parseError.Error())
        
        ssIterator.Release()
        
        return false
    }
    
    ssIterator.currentKey = ssIterator.dbIterator.Key()
    ssIterator.currentValue = &siblingSet
    
    return true
}

func (ssIterator *BasicSiblingSetIterator) Prefix() []byte {
    return decodePartitionDataKey(ssIterator.dbIterator.Prefix())
}

func (ssIterator *BasicSiblingSetIterator) Key() []byte {
    if ssIterator.currentKey == nil {
        return nil
    }
    
    return decodePartitionDataKey(ssIterator.currentKey)
}

func (ssIterator *BasicSiblingSetIterator) Value() *SiblingSet {
    return ssIterator.currentValue
}

func (ssIterator *BasicSiblingSetIterator) Release() {
    ssIterator.dbIterator.Release()
}

func (ssIterator *BasicSiblingSetIterator) Error() error {
    if ssIterator.parseError != nil {
        return EStorage
    }
        
    if ssIterator.dbIterator.Error() != nil {
        return EStorage
    }
    
    return nil
}