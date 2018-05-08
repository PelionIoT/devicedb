package storage

import (
    "strings"
    "errors"
    "sort"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    "github.com/syndtr/goleveldb/leveldb/iterator"
    "github.com/syndtr/goleveldb/leveldb/util"
    levelErrors "github.com/syndtr/goleveldb/leveldb/errors"

    . "devicedb/error"
    . "devicedb/logging"
)

const (
    PUT = iota
    DEL = iota
    FORWARD = iota
    BACKWARD = iota
)

var (
    SnapshotBatchSize = 1000
    SnapshotBatchMaxBytes = 5 * 1024 * 1024 // 5 MB
)

type Op struct {
    OpType int `json:"type"`
    OpKey []byte `json:"key"`
    OpValue []byte `json:"value"`
}

func (o *Op) IsDelete() bool {
    return o.OpType == DEL
}

func (o *Op) IsPut() bool {
    return o.OpType == PUT
}

func (o *Op) Key() []byte {
    return o.OpKey
}

func (o *Op) Value() []byte {
    return o.OpValue
}

type OpList []Op

func (opList OpList) Len() int {
    return len(opList)
}

func (opList OpList) Less(i, j int) bool {
    k1 := opList[i].Key()
    k2 := opList[i].Key()
    
    for i := 0; i < len(k1) && i < len(k2); i += 1 {
        if k2[i] > k1[i] {
            return false
        }
    }
    
    return true
}

func (opList OpList) Swap(i, j int) {
    opList[i], opList[j] = opList[j], opList[i]
}

type Batch struct {
    BatchOps map[string]Op `json:"ops"`
}

func NewBatch() *Batch {
    return &Batch{ make(map[string]Op) }
}

func (batch *Batch) Put(key []byte, value []byte) *Batch {
    batch.BatchOps[string(key)] = Op{ PUT, key, value }
    
    return batch
}

func (batch *Batch) Delete(key []byte) *Batch {
    batch.BatchOps[string(key)] = Op{ DEL, key, nil }
    
    return batch
}

func (batch *Batch) Ops() map[string]Op {
    return batch.BatchOps
}

func (batch *Batch) SortedOps() []Op {
    opList := make([]Op, 0, len(batch.BatchOps))
    
    for _, op := range batch.BatchOps {
        opList = append(opList, op)
    }
    
    sort.Sort(OpList(opList))
    
    return opList
}

type StorageIterator interface {
    Next() bool
    Prefix() []byte
    Key() []byte
    Value() []byte
    Release()
    Error() error
}

type PrefixedStorageDriver struct {
    prefix []byte
    storageDriver StorageDriver
}

func NewPrefixedStorageDriver(prefix []byte, storageDriver StorageDriver) *PrefixedStorageDriver {
    return &PrefixedStorageDriver{ prefix, storageDriver }
}

func (psd *PrefixedStorageDriver) Open() error {
    return nil
}

func (psd *PrefixedStorageDriver) Close() error {
    return nil
}

func (psd *PrefixedStorageDriver) Recover() error {
    return psd.storageDriver.Recover()
}

func (psd *PrefixedStorageDriver) Compact() error {
    return psd.storageDriver.Compact()
}

func (psd *PrefixedStorageDriver) addPrefix(k []byte) []byte {
    result := make([]byte, 0, len(psd.prefix) + len(k))
        
    result = append(result, psd.prefix...)
    result = append(result, k...)
    
    return result
}

func (psd *PrefixedStorageDriver) Get(keys [][]byte) ([][]byte, error) {
    prefixKeys := make([][]byte, len(keys))
    
    for i, _ := range keys {
        prefixKeys[i] = psd.addPrefix(keys[i])
    }
    
    return psd.storageDriver.Get(prefixKeys)
}

func (psd *PrefixedStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
    prefixKeys := make([][]byte, len(keys))
    
    for i, _ := range keys {
        prefixKeys[i] = psd.addPrefix(keys[i])
    }
    
    iter, err := psd.storageDriver.GetMatches(prefixKeys)
    
    if err != nil {
        return nil, err
    }
    
    return &PrefixedIterator{ psd.prefix, iter }, nil
}

func (psd *PrefixedStorageDriver) GetRange(start []byte, end []byte) (StorageIterator, error) {
    iter, err := psd.storageDriver.GetRange(psd.addPrefix(start), psd.addPrefix(end))
    
    if err != nil {
        return nil, err
    }
    
    return &PrefixedIterator{ psd.prefix, iter }, nil
}

func (psd *PrefixedStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
    var prefixedRanges = make([][2][]byte, len(ranges))
    
    for i := 0; i < len(ranges); i += 1 {
        prefixedRanges[i] = [2][]byte{ psd.addPrefix(ranges[i][0]), psd.addPrefix(ranges[i][1]) }
    }
    
    iter, err := psd.storageDriver.GetRanges(prefixedRanges, direction)
    
    if err != nil {
        return nil, err
    }
    
    return &PrefixedIterator{ psd.prefix, iter }, nil
}

func (psd *PrefixedStorageDriver) Batch(batch *Batch) error {
    newBatch := NewBatch()
    
    for key, op := range batch.BatchOps {
        op.OpKey = psd.addPrefix([]byte(key))
        newBatch.BatchOps[string(psd.addPrefix([]byte(key)))] = op
    }
    
    return psd.storageDriver.Batch(newBatch)
}

func (psd *PrefixedStorageDriver) Snapshot(snapshotDirectory string) error {
    return psd.storageDriver.Snapshot(snapshotDirectory)
}

type PrefixedIterator struct {
    prefix []byte
    iterator StorageIterator
}

func NewPrefixedIterator(iter StorageIterator, prefix []byte) *PrefixedIterator {
    return &PrefixedIterator{ prefix, iter }
}

func (prefixedIterator *PrefixedIterator) Next() bool {
    return prefixedIterator.iterator.Next()
}

func (prefixedIterator *PrefixedIterator) Prefix() []byte {
    return prefixedIterator.iterator.Prefix()[len(prefixedIterator.prefix):]
}

func (prefixedIterator *PrefixedIterator) Key() []byte {
    return prefixedIterator.iterator.Key()[len(prefixedIterator.prefix):]
}

func (prefixedIterator *PrefixedIterator) Value() []byte {
    return prefixedIterator.iterator.Value()
}

func (prefixedIterator *PrefixedIterator) Release() {
    prefixedIterator.iterator.Release()
}

func (prefixedIterator *PrefixedIterator) Error() error {
    return prefixedIterator.iterator.Error()
}

type StorageDriver interface {
    Open() error
    Close() error
    Recover() error
    Compact() error
    Get([][]byte) ([][]byte, error)
    GetMatches([][]byte) (StorageIterator, error)
    GetRange([]byte, []byte) (StorageIterator, error)
    GetRanges([][2][]byte, int) (StorageIterator, error)
    Batch(*Batch) error
    Snapshot(snapshotDirectory string) error
}

type LevelDBIterator struct {
    snapshot *leveldb.Snapshot
    it iterator.Iterator
    ranges []*util.Range
    prefix []byte
    err error
    direction int
}

func (it *LevelDBIterator) Next() bool {
    if it.it == nil {
        if len(it.ranges) == 0 {
            return false
        }
    
        it.prefix = it.ranges[0].Start
        it.it = it.snapshot.NewIterator(it.ranges[0], nil)
        it.ranges = it.ranges[1:]
        
        if it.direction == BACKWARD {
            if it.it.Last() {
                return true
            }
            
            if it.it.Error() != nil {
                it.err = it.it.Error()
                it.ranges = []*util.Range{ }
            }
            
            it.it.Release()
            it.it = nil
            it.prefix = nil
            
            return false
        }
    }

    if it.direction == BACKWARD {
        if it.it.Prev() {
            return true
        }
    } else {
        if it.it.Next() {
            return true
        }
    }
    
    if it.it.Error() != nil {
        it.err = it.it.Error()
        it.ranges = []*util.Range{ }
    }
    
    it.it.Release()
    it.it = nil
    it.prefix = nil
    
    return it.Next()
}

func (it *LevelDBIterator) Prefix() []byte {
    return it.prefix
}

func (it *LevelDBIterator) Key() []byte {
    if it.it == nil || it.err != nil {
        return nil
    }
    
    return it.it.Key()
}

func (it *LevelDBIterator) Value() []byte {
    if it.it == nil || it.err != nil {
        return nil
    }
    
    return it.it.Value()
}

func (it *LevelDBIterator) Release() {
    it.prefix = nil
    it.ranges = []*util.Range{ }
    it.snapshot.Release()
    
    if it.it == nil {
        return
    }
    
    it.it.Release()
    it.it = nil
}

func (it *LevelDBIterator) Error() error {
    return it.err
}

type LevelDBStorageDriver struct {
    file string
    options *opt.Options
    db *leveldb.DB
}

func NewLevelDBStorageDriver(file string, options *opt.Options) *LevelDBStorageDriver {
    return &LevelDBStorageDriver{ file, options, nil }
}

func (levelDriver *LevelDBStorageDriver) Open() error {
    levelDriver.Close()
    
    db, err := leveldb.OpenFile(levelDriver.file, levelDriver.options)
    
    if err != nil {
        if levelErrors.IsCorrupted(err) {
            Log.Criticalf("LevelDB database is corrupted: %v", err.Error())

            return ECorrupted
        }

        return err
    }
    
    levelDriver.db = db
    
    return nil
}

func (levelDriver *LevelDBStorageDriver) Close() error {
    if levelDriver.db == nil {
        return nil
    }
    
    err := levelDriver.db.Close()

    levelDriver.db = nil

    return err
}

func (levelDriver *LevelDBStorageDriver) Recover() error {
    levelDriver.Close()

    db, err := leveldb.RecoverFile(levelDriver.file, levelDriver.options)

    if err != nil {
        return err
    }

    levelDriver.db = db

    return nil
}

func (levelDriver *LevelDBStorageDriver) Compact() error {
    if levelDriver.db == nil {
        return errors.New("Driver is closed")
    }

    err := levelDriver.db.CompactRange(util.Range{ })

    if err != nil {
        return err
    }

    return nil
}

func (levelDriver *LevelDBStorageDriver) Get(keys [][]byte) ([][]byte, error) {
    if levelDriver.db == nil {
        return nil, errors.New("Driver is closed")
    }
    
    if keys == nil {
        return [][]byte{ }, nil
    }

    snapshot, err := levelDriver.db.GetSnapshot()
    
    defer snapshot.Release()
    
    if err != nil {
        return nil, err
    }
    
    values := make([][]byte, len(keys))
    
    for i, key := range keys {
        if key == nil {
            values[i] = nil
        } else {
            values[i], err = snapshot.Get(key, &opt.ReadOptions{ false, opt.DefaultStrict })
            
            if err != nil {
                if err.Error() != "leveldb: not found" {
                    return nil, err
                } else {
                    values[i] = nil
                }
            }
        }
    }
    
    return values, nil
}

func consolidateKeys(keys [][]byte) [][]byte {
    if keys == nil {
        return [][]byte{ }
    }
    
    s := make([]string, 0, len(keys))

    for _, key := range keys {
        if key == nil {
            continue
        }
        
        s = append(s, string([]byte(key)))
    }
    
    sort.Strings(s)
    
    result := make([][]byte, 0, len(s))
    
    for i := 0; i < len(s); i += 1 {
        if i == 0 {
            result = append(result, []byte(s[i]))
            continue
        }
        
        if !strings.HasPrefix(s[i], s[i - 1]) {
            result = append(result, []byte(s[i]))
        } else {
            s[i] = s[i - 1]
        }
    }
    
    return result
}

func (levelDriver *LevelDBStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
    if levelDriver.db == nil {
        return nil, errors.New("Driver is closed")
    }
    
    keys = consolidateKeys(keys)
    snapshot, err := levelDriver.db.GetSnapshot()
    
    if err != nil {
        snapshot.Release()
        
        return nil, err
    }

    ranges := make([]*util.Range, 0, len(keys))
    
    if keys == nil {
        return &LevelDBIterator{ snapshot, nil, ranges, nil, nil, FORWARD }, nil
    }
    
    for _, key := range keys {
        if key == nil {
            continue
        } else {
            ranges = append(ranges, util.BytesPrefix(key))
        }
    }

    return &LevelDBIterator{ snapshot, nil, ranges, nil, nil, FORWARD }, nil
}

func (levelDriver *LevelDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
    if levelDriver.db == nil {
        return nil, errors.New("Driver is closed")
    }
    
    snapshot, err := levelDriver.db.GetSnapshot()
    
    if err != nil {
        snapshot.Release()
        
        return nil, err
    }

    ranges := []*util.Range{ &util.Range{ min, max } }
    
    return &LevelDBIterator{ snapshot, nil, ranges, nil, nil, FORWARD }, nil
}

func (levelDriver *LevelDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
    if levelDriver.db == nil {
        return nil, errors.New("Driver is closed")
    }
    
    snapshot, err := levelDriver.db.GetSnapshot()
    
    if err != nil {
        snapshot.Release()
        
        return nil, err
    }
    
    var levelRanges = make([]*util.Range, len(ranges))
    
    for i := 0; i < len(ranges); i += 1 {
        levelRanges[i] = &util.Range{ ranges[i][0], ranges[i][1] }
    }

    return &LevelDBIterator{ snapshot, nil, levelRanges, nil, nil, direction }, nil
}

func (levelDriver *LevelDBStorageDriver) Batch(batch *Batch) error {
    if levelDriver.db == nil {
        return errors.New("Driver is closed")
    }
    
    if batch == nil {
        return nil
    }
    
    b := new(leveldb.Batch)
    ops := batch.Ops()
    
    for _, op := range ops {
        if op.OpType == PUT {
            b.Put(op.Key(), op.Value())
        } else if op.OpType == DEL {
            b.Delete(op.Key())
        } 
    }
    
    return levelDriver.db.Write(b, nil)
}

func (levelDriver *LevelDBStorageDriver) Snapshot(snapshotDirectory string) error {
    if levelDriver.db == nil {
        return errors.New("Driver is closed")
    }
    
    snapshotDB, err := leveldb.OpenFile(snapshotDirectory, &opt.Options{ })
    
    if err != nil {
        Log.Errorf("Can't create snapshot because %s could not be opened for writing: %v", snapshotDirectory, err)
        
        return err
    }

    snapshot, err := levelDriver.db.GetSnapshot()

    defer snapshot.Release()
    
    if err != nil {
        Log.Errorf("Can't create snapshot because a read snapshot could not be obtained from underlying level driver: %v", err)

        return err
    }
    
    iter := snapshot.NewIterator(&util.Range{}, &opt.ReadOptions{ DontFillCache: true })

    defer iter.Release()

    var batch *leveldb.Batch = &leveldb.Batch{}
    var batchSizeBytes int
    var totalKeys uint64

    Log.Debugf("Iterating through database to create a snapshot at %s", snapshotDirectory)

    for iter.Next() {
        totalKeys++
        batch.Put(iter.Key(), iter.Value())
        batchSizeBytes += len(iter.Key()) + len(iter.Value())
        
        if batchSizeBytes >= SnapshotBatchMaxBytes || batch.Len() >= SnapshotBatchSize {
            Log.Debugf("Writing next snapshot chunk (batch.Len() = %d, batchSizeBytes = %d, totalKeys = %d)", batch.Len(), batchSizeBytes, totalKeys)

            if err := snapshotDB.Write(batch, &opt.WriteOptions{ Sync: true }); err != nil {
                Log.Errorf("Can't create snapshot because there was a problem writing the next chunk to snapshot: %v", err)

                return err
            }

            batchSizeBytes = 0
            batch.Reset()
        }
    }

    if iter.Error() != nil {
        Log.Errorf("Can't create snapshot because there was an iterator error: %v", iter.Error())

        return iter.Error()
    }

    // Write the rest of the records in one last batch
    if batch.Len() > 0 {
        Log.Debugf("Writing next snapshot chunk (batch.Len() = %d, batchSizeBytes = %d, totalKeys = %d)", batch.Len(), batchSizeBytes, totalKeys)

        if err := snapshotDB.Write(batch, &opt.WriteOptions{ Sync: true }); err != nil {
            Log.Errorf("Can't create snapshot because there was a problem writing the next chunk to snapshot: %v", err)

            return err
        }
    }

    if err := snapshotDB.Close(); err != nil {
        Log.Errorf("Can't create snapshot because there was an error while closing the snapshot database at %s: %v", snapshotDirectory, err)

        return err
    }

    Log.Debugf("Created snapshot at %s", snapshotDirectory)    

    return nil
}
