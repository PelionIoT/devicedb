package storage

import "sort"

const (
	PUT      = iota
	DEL      = iota
	FORWARD  = iota
	BACKWARD = iota
)

var (
	CopyBatchSize     = 1000
	CopyBatchMaxBytes = 5 * 1024 * 1024 // 5 MB
)

type StorageIterator interface {
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
	Recover() error
	Compact() error
	Get([][]byte) ([][]byte, error)
	GetMatches([][]byte) (StorageIterator, error)
	GetRange([]byte, []byte) (StorageIterator, error)
	GetRanges([][2][]byte, int) (StorageIterator, error)
	Batch(*Batch) error
	Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error
	OpenSnapshot(snapshotDirectory string) (StorageDriver, error)
	Restore(storageDriver StorageDriver) error
}

type Op struct {
	OpType  int    `json:"type"`
	OpKey   []byte `json:"key"`
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
	return &Batch{make(map[string]Op)}
}

func (batch *Batch) Size() int {
	return len(batch.BatchOps)
}

func (batch *Batch) Put(key []byte, value []byte) *Batch {
	batch.BatchOps[string(key)] = Op{PUT, key, value}

	return batch
}

func (batch *Batch) Delete(key []byte) *Batch {
	batch.BatchOps[string(key)] = Op{DEL, key, nil}

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
