package storage

type BoltDBStorageIterator struct {
}

func (bt *BoltDBStorageIterator) Next() bool {
	return true
}

func (bt *BoltDBStorageIterator) Prefix() []byte {
	return nil
}

func (bt *BoltDBStorageIterator) Key() []byte {
	return nil
}

func (bt *BoltDBStorageIterator) Value() []byte {
	return nil
}

func (bt *BoltDBStorageIterator) Release() {

}

func (bt *BoltDBStorageIterator) Error() error {
	return nil
}

type BoltDBStorageDriver struct {
}

func NewBoltDBStorageDriver() *BoltDBStorageDriver {
	return &BoltDBStorageDriver{}
}

func (bd *BoltDBStorageDriver) Open() error {
	return nil
}

func (bd *BoltDBStorageDriver) Close() error {
	return nil
}

func (bd *BoltDBStorageDriver) Recover() error {
	return nil
}

func (bd *BoltDBStorageDriver) Compact() error {
	return nil
}

func (bd *BoltDBStorageDriver) Get(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))

	return values, nil
}

func (bd *BoltDBStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (bd *BoltDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (bd *BoltDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (bd *BoltDBStorageDriver) Batch(batch *Batch) error {
	return nil
}

func (bd *BoltDBStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	return nil
}

func (bd *BoltDBStorageDriver) OpenSnapshot(snapshotDirectory string) (StorageDriver, error) {
	snapshotDB := NewBoltDBStorageDriver()

	return snapshotDB, nil
}

func (bd *BoltDBStorageDriver) Restore(storageDriver StorageDriver) error {
	return nil
}
