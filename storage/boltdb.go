package storage

import (
	bolt "go.etcd.io/bbolt"
)

type utilRange struct {
	start []byte
	limit []byte
}

type BoltDBStorageIterator struct {
	bucket    bolt.Bucket
	cursor    *bolt.Cursor
	ranges    []*utilRange
	prefix    []byte
	key       []byte
	value     []byte
	direction int
}

func (iter *BoltDBStorageIterator) Next() bool {
	if iter.cursor == nil {
		if len(iter.ranges) == 0 {
			return false
		}

		iter.prefix = iter.ranges[0].start
		iter.cursor = iter.bucket.Cursor()
		iter.ranges = iter.ranges[1:]

		if iter.direction == BACKWARD {
			if k, v := iter.cursor.Last(); k != nil {
				iter.key, iter.value = k, v
				return true
			}

			iter.Release()

			return false
		}
	}

	if iter.direction == BACKWARD {
		if k, v := iter.cursor.Next(); k != nil {
			iter.key, iter.value = k, v
			return true
		}
	} else {
		if k, v := iter.cursor.Next(); k != nil {
			iter.key, iter.value = k, v
			return true
		}
	}

	iter.Release()

	return iter.Next()
}

func (iter *BoltDBStorageIterator) Prefix() []byte {
	return iter.prefix
}

func (iter *BoltDBStorageIterator) Key() []byte {
	return iter.key
}

func (iter *BoltDBStorageIterator) Value() []byte {
	return iter.value
}

func (iter *BoltDBStorageIterator) Release() {
	iter.prefix = nil
	iter.key = nil
	iter.value = nil
	iter.cursor = nil
}

func (iter *BoltDBStorageIterator) Error() error {
	return nil
}

type BoltDBStorageDriver struct {
}

func NewBoltDBStorageDriver() *BoltDBStorageDriver {
	return &BoltDBStorageDriver{}
}

func (driver *BoltDBStorageDriver) Open() error {
	return nil
}

func (driver *BoltDBStorageDriver) Close() error {
	return nil
}

func (driver *BoltDBStorageDriver) Recover() error {
	return nil
}

func (driver *BoltDBStorageDriver) Compact() error {
	return nil
}

func (driver *BoltDBStorageDriver) Get(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))

	return values, nil
}

func (driver *BoltDBStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (driver *BoltDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (driver *BoltDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	return &BoltDBStorageIterator{}, nil
}

func (driver *BoltDBStorageDriver) Batch(batch *Batch) error {
	return nil
}

func (driver *BoltDBStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	return nil
}

func (driver *BoltDBStorageDriver) OpenSnapshot(snapshotDirectory string) (StorageDriver, error) {
	snapshotDB := NewBoltDBStorageDriver()

	return snapshotDB, nil
}

func (driver *BoltDBStorageDriver) Restore(storageDriver StorageDriver) error {
	return nil
}
