package halodb

import (
    "strings"
    "errors"
    "sort"
    
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/opt"
    "github.com/syndtr/goleveldb/leveldb/iterator"
    "github.com/syndtr/goleveldb/leveldb/util"
)

const (
    PUT = iota
    DEL = iota
)

type op struct {
    opType int
    key []byte
    value []byte
}

func (o *op) IsDelete() bool {
    return o.opType == DEL
}

func (o *op) IsPut() bool {
    return o.opType == PUT
}

func (o *op) Key() []byte {
    return o.key
}

func (o *op) Value() []byte {
    return o.value
}

type OpList []op

func (opList OpList) Len() int {
    return len(opList)
}

func (opList OpList) Less(i, j int) bool {
    k1 := opList[i].key
    k2 := opList[i].key
    
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
    ops map[string]op
}

func NewBatch() *Batch {
    return &Batch{ make(map[string]op) }
}

func (batch *Batch) Put(key []byte, value []byte) *Batch {
    batch.ops[string(key)] = op{ PUT, key, value }
    
    return batch
}

func (batch *Batch) Delete(key []byte) *Batch {
    batch.ops[string(key)] = op{ DEL, key, nil }
    
    return batch
}

func (batch *Batch) Ops() map[string]op {
    return batch.ops
}

func (batch *Batch) SortedOps() []op {
    opList := make([]op, 0, len(batch.ops))
    
    for _, op := range batch.ops {
        opList = append(opList, op)
    }
    
    sort.Sort(OpList(opList))
    
    return opList
}

type Iterator interface {
    Next() bool
    Prefix() []byte
    Key() []byte
    Value() []byte
    Release()
    Error() error
}

type StorageDriver interface {
    Open() error
    Close() error
    Get([][]byte) ([][]byte, error)
    GetMatches([][]byte) (Iterator, error)
    GetRange([]byte, []byte) (Iterator, error)
    Batch(*Batch) error
}

type LevelDBIterator struct {
    snapshot *leveldb.Snapshot
    it iterator.Iterator
    ranges []*util.Range
    prefix []byte
    err error
}

func (it *LevelDBIterator) Next() bool {
    if it.it == nil {
        if len(it.ranges) == 0 {
            return false
        }
    
        it.prefix = it.ranges[0].Start
        it.it = it.snapshot.NewIterator(it.ranges[0], nil)
        it.ranges = it.ranges[1:]
    }
    
    if it.it.Next() {
        return true
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

func (levelDriver *LevelDBStorageDriver) GetMatches(keys [][]byte) (Iterator, error) {
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
        return &LevelDBIterator{ snapshot, nil, ranges, nil, nil }, nil
    }
    
    for _, key := range keys {
        if key == nil {
            continue
        } else {
            ranges = append(ranges, util.BytesPrefix(key))
        }
    }

    return &LevelDBIterator{ snapshot, nil, ranges, nil, nil }, nil
}

func (levelDriver *LevelDBStorageDriver) GetRange(min, max []byte) (Iterator, error) {
    if levelDriver.db == nil {
        return nil, errors.New("Driver is closed")
    }
    
    snapshot, err := levelDriver.db.GetSnapshot()
    
    if err != nil {
        snapshot.Release()
        
        return nil, err
    }

    ranges := []*util.Range{ &util.Range{ min, max } }
    
    return &LevelDBIterator{ snapshot, nil, ranges, nil, nil }, nil
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
        if op.opType == PUT {
            b.Put(op.key, op.value)
        } else if op.opType == DEL {
            b.Delete(op.key)
        } 
    }
    
    return levelDriver.db.Write(b, nil)
}
