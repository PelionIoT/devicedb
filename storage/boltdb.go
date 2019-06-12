package storage

import (
	"errors"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type utilRange struct {
	start []byte
	end   []byte
}

type BoltDBStorageIterator struct {
	db         *bolt.DB
	rootBucket string
	cursor     *bolt.Cursor
	ranges     []*utilRange
	prefix     []byte
	key        []byte
	value      []byte
	direction  int
}

func (iter *BoltDBStorageIterator) Next() bool {
	// if iter.cursor == nil {
	// 	if len(iter.ranges) == 0 {
	// 		return false
	// 	}

	// 	iter.prefix = iter.ranges[0].start
	// 	iter.cursor = iter.bucket.Cursor() //
	// 	iter.ranges = iter.ranges[1:]

	// 	if iter.direction == BACKWARD {
	// 		if k, v := iter.cursor.Last(); k != nil {
	// 			iter.key, iter.value = k, v
	// 			return true
	// 		}

	// 		iter.Release()

	// 		return false
	// 	}
	// }

	// if iter.direction == BACKWARD {
	// 	if k, v := iter.cursor.Next(); k != nil {
	// 		iter.key, iter.value = k, v
	// 		return true
	// 	}
	// } else {
	// 	if k, v := iter.cursor.Next(); k != nil {
	// 		iter.key, iter.value = k, v
	// 		return true
	// 	}
	// }

	// iter.Release()

	// return iter.Next()
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
	file       string
	rootBucket string
	options    *bolt.Options
	db         *bolt.DB
}

func NewBoltDBStorageDriver(file string, rootBucket string, options *bolt.Options) *BoltDBStorageDriver {
	return &BoltDBStorageDriver{file, rootBucket, options, nil, nil}
}

func (driver *BoltDBStorageDriver) Open() error {
	driver.Close()

	db, err := bolt.Open(driver.file, 0666, driver.options)
	if err != nil {
		// log

		return err
	}

	driver.db = db

	return nil
}

func (driver *BoltDBStorageDriver) Close() error {
	if driver.db == nil {
		return nil
	}

	err := driver.db.Close()

	driver.db = nil

	return nil
}

func (driver *BoltDBStorageDriver) Recover() error {
	return driver.Open()
}

func (driver *BoltDBStorageDriver) Compact() error {
	if driver.db == nil {
		return errors.New("Driver is closed")
	}

	return nil
}

func (driver *BoltDBStorageDriver) Get(keys [][]byte) ([][]byte, error) {
	if driver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	if keys == nil {
		return [][]byte{}, nil
	}

	values := make([][]byte, len(keys))

	// use view transaction here as this is a read operation and it would act like the snapshot of leveldb
	if err := driver.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(driver.rootBucket))
		if bucket == nil {
			// log here: only retrieves values when the root bucket exists

			return errors.New("No such bucket")
		}

		for i, key := range keys {
			if key == nil {
				values[i] = nil
			} else {
				values[i] = bucket.Get(key)
			}
		}

		return nil
	}); err != nil {
		// log here: something went wrong here, which is the error that No such bucket

		return nil, err
	}

	return values, nil
}

func consolidateKeys(keys [][]byte) [][]byte {
	if keys == nil {
		return [][]byte{}
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

	for i := 0; i < len(s); i++ {
		if i == 0 {
			result = append(result, []byte(s[i]))
			continue
		}

		if !strings.HasPrefix(s[i], s[i-1]) {
			result = append(result, []byte(s[i]))
		} else {
			s[i] = s[i-1]
		}
	}

	return result
}

func bytesPrefix(prefix []byte) *utilRange {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return &utilRange{prefix, limit}
}

func (driver *BoltDBStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
	if driver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	keys = consolidateKeys(keys)

	ranges := make([]*utilRange, 0, len(keys))
	if keys == nil {
		return &BoltDBStorageIterator{driver.db, driver.rootBucket, nil, ranges, nil, nil, nil, FORWARD}, nil
	}

	for _, key := range keys {
		if key == nil {
			continue
		} else {
			ranges = append(ranges, bytesPrefix(key))
		}
	}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, nil, ranges, nil, nil, nil, FORWARD}, nil
}

func (driver *BoltDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
	if driver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	ranges := []*utilRange{&utilRange{min, max}}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, nil, ranges, nil, nil, nil, FORWARD}, nil
}

func (driver *BoltDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	if driver.db == nil {
		return nil, errors.New("Drives is closed")
	}

	levelRanges := make([]*utilRange, len(ranges))

	for i := 0; i < len(ranges); i++ {
		levelRanges[i] = &utilRange{ranges[i][0], ranges[i][1]}
	}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, nil, levelRanges, nil, nil, nil, direction}, nil
}

func (driver *BoltDBStorageDriver) Batch(batch *Batch) error {
	if driver.db == nil {
		return errors.New("Driver is closed")
	}

	if batch == nil {
		return nil
	}

	var bucket *bolt.Bucket
	var err error

	if err = driver.db.Update(func(tx *bolt.Tx) error {
		if bucket = tx.Bucket([]byte(driver.rootBucket)); bucket == nil {
			if bucket, err = tx.CreateBucket([]byte(driver.rootBucket)); err != nil {
				// log here: something went wrong when try to create the bucket

				return err
			}
		}

		ops := batch.Ops()
		for _, op := range ops {
			if op.OpType == PUT {
				bucket.Put(op.Key(), op.Value())
			} else if op.OpType == DEL {
				bucket.Delete(op.Key())
			}
		}

		return nil
	}); err != nil {
		// log here: something went wrong when trying to insert or delete date

		return err
	}

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
