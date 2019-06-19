package storage

import (
	. "devicedb/logging"
	"errors"
	"os"

	bolt "go.etcd.io/bbolt"
)

type utilRange struct {
	start []byte
	end   []byte
}

type BoltDBStorageIterator struct {
	db         *bolt.DB
	rootBucket string
	ranges     []*utilRange
	prefix     []byte
	key        []byte
	value      []byte
	direction  int
	err        error
}

func (it *BoltDBStorageIterator) Next() bool {
	var tx *bolt.Tx
	var err error

	if tx, err = it.db.Begin(false); err != nil {
		prometheusRecordStorageError("iterator.next()", "")
		it.err = err

		return false
	}
	defer tx.Rollback()

	var b *bolt.Bucket
	if b = tx.Bucket([]byte(it.rootBucket)); b == nil {
		prometheusRecordStorageError("iterator.next()", "")
		it.err = errors.New("there is no such bucket in BoltDB")

		return false
	}

	cursor := b.Cursor()

	if it.key == nil {
		if len(it.ranges) == 0 {
			return false
		}

		it.prefix = it.ranges[0].start
		it.ranges = it.ranges[1:]
		it.key = it.prefix

		if it.direction == BACKWARD {
			if k, v := cursor.Last(); k != nil {
				copy(it.key, k)
				copy(it.value, v)

				return true
			}

			it.Release()

			return false
		}
	}

	curKey, _ := cursor.Seek(it.key)

	if it.direction == BACKWARD {
		if curKey != nil && string(curKey) != string(it.prefix) {
			prevKey, prevValue := cursor.Prev()
			copy(it.key, prevKey)
			copy(it.value, prevValue)

			if prevKey != nil {
				return true
			}
		}
	} else {
		if curKey != nil && string(curKey) != string(it.ranges[0].end) {
			nextKey, nextValue := cursor.Next()
			copy(it.key, nextKey)
			copy(it.value, nextValue)

			if nextKey != nil {
				return true
			}
		}
	}

	it.Release()

	return it.Next()
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
	iter.ranges = []*utilRange{}
}

func (iter *BoltDBStorageIterator) Error() error {
	return iter.err
}

type BoltDBStorageDriver struct {
	db         *bolt.DB
	file       string
	mode       os.FileMode
	rootBucket string
	options    *bolt.Options
}

func NewBoltDBStorageDriver(file string, rootBucket string, mode os.FileMode, options *bolt.Options) *BoltDBStorageDriver {
	return &BoltDBStorageDriver{nil, file, mode, rootBucket, options}
}

func (driver *BoltDBStorageDriver) Open() error {
	driver.Close()

	db, err := bolt.Open(driver.file, driver.mode, driver.options)
	if err != nil {
		prometheusRecordStorageError("open()", driver.file)

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

	return err
}

func (driver *BoltDBStorageDriver) Recover() error {
	driver.Close()

	db, err := bolt.Open(driver.file, driver.mode, driver.options)
	if err != nil {
		prometheusRecordStorageError("recover()", driver.file)

		return err
	}

	driver.db = db

	return nil
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

	if err := driver.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(driver.rootBucket))
		if bucket == nil {
			prometheusRecordStorageError("get()", driver.file)

			return errors.New("No such bucket")
		}

		for i, key := range keys {
			copy(values[i], bucket.Get(key))
		}

		return nil
	}); err != nil {
		prometheusRecordStorageError("get()", driver.file)

		return nil, err
	}

	return values, nil
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
		return &BoltDBStorageIterator{driver.db, driver.rootBucket, ranges, nil, nil, nil, FORWARD, nil}, nil
	}

	for _, key := range keys {
		if key == nil {
			continue
		} else {
			ranges = append(ranges, bytesPrefix(key))
		}
	}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, ranges, nil, nil, nil, FORWARD, nil}, nil
}

func (driver *BoltDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
	if driver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	ranges := []*utilRange{&utilRange{min, max}}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, ranges, nil, nil, nil, FORWARD, nil}, nil
}

func (driver *BoltDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	if driver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	levelRanges := make([]*utilRange, len(ranges))

	for i := 0; i < len(ranges); i++ {
		levelRanges[i] = &utilRange{ranges[i][0], ranges[i][1]}
	}

	return &BoltDBStorageIterator{driver.db, driver.rootBucket, levelRanges, nil, nil, nil, direction, nil}, nil
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
				prometheusRecordStorageError("batch()", driver.file)

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
		prometheusRecordStorageError("batch()", driver.file)

		return err
	}

	return nil
}

func (driver *BoltDBStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	if driver.db == nil {
		return errors.New("Driver is closed")
	}

	snapshotDB, err := bolt.Open(snapshotDirectory, driver.mode, &bolt.Options{})
	if err != nil {
		prometheusRecordStorageError("snapshot()", driver.file)

		Log.Errorf("Can't create snapshot because %s could not be opened for writing: %v", snapshotDirectory, err)

		return err
	}

	Log.Debugf("Copying database contents to snapshot at %s", snapshotDirectory)

	//If a long running read transaction (for example, a snapshot transaction) is needed, you might want to set DB.InitialMmapSize to a large enough value to avoid potential blocking of write transaction.
	if err := boltCopy(driver.rootBucket, snapshotDB, driver.db); err != nil {
		prometheusRecordStorageError("snapshot()", driver.file)

		Log.Errorf("Can't create snapshot because there was an error while copying the keys: %v", err)

		return err
	}

	Log.Debugf("Recording snapshot metadata: %v", metadata)

	if err := snapshotDB.Update(func(tx *bolt.Tx) error {
		var snapshotBucket *bolt.Bucket

		if snapshotBucket = tx.Bucket([]byte(driver.rootBucket)); snapshotBucket == nil {
			prometheusRecordStorageError("snapshot()", driver.file)

			Log.Error("Can't find the root bucket in the snapshot: %v", snapshotDirectory)

			return errors.New("Can't create snapshot because there was a problem opening the root bucket in the snapshot")
		}

		Log.Debugf("Recording snapshot metadata: %v", metadata)

		// Now write the snapshot metadata
		for metaKey, metaValue := range metadata {
			key := make([]byte, len(metadataPrefix)+len([]byte(metaKey)))

			copy(key, metadataPrefix)
			copy(key[len(metadataPrefix):], []byte(metaKey))

			if err := snapshotBucket.Put(key, []byte(metaValue)); err != nil {
				prometheusRecordStorageError("snapshot()", driver.file)

				Log.Errorf("Can't create snapshot because there was a problem recording the snapshot metadata: %v", err)

				return err
			}
		}

		return nil
	}); err != nil {
		prometheusRecordStorageError("snapshot()", driver.file)

		Log.Errorf("Can't create the snapshot because there was a problem opening the snapshot at %v", snapshotDirectory)

		return err
	}

	if snapshotDB.Close(); err != nil {
		prometheusRecordStorageError("snapshot()", driver.file)

		Log.Errorf("Can't create snapshot because there was an error while closing the snapshot database at %s: %v", snapshotDirectory, err)

		return err
	}

	Log.Debugf("Created snapshot at %s", snapshotDirectory)

	return nil
}

func boltCopy(rootBucket string, dest *bolt.DB, src *bolt.DB) error {
	var err error
	var srcBucket *bolt.Bucket
	var destBucket *bolt.Bucket

	if err = src.View(func(tx *bolt.Tx) error {
		if srcBucket = tx.Bucket([]byte(rootBucket)); srcBucket == nil {
			Log.Error("Can't find the root bucket in the source BoltDB when trying to create the snapshotDB from it")

			return errors.New("There is no root bucket in the source DB when trying to create the snapshot DB")
		}

		if err = dest.Update(func(tx *bolt.Tx) error {
			if destBucket = tx.Bucket([]byte(rootBucket)); destBucket == nil {
				if destBucket, err = tx.CreateBucketIfNotExists([]byte(rootBucket)); err != nil {
					Log.Errorf("Can't create the root bucket in the snapshot DB when trying to copy the key-value pair to snapshotDB")

					return err
				}
			}

			if err = srcBucket.ForEach(func(k, v []byte) error {
				if err = destBucket.Put(k, v); err != nil {
					Log.Errorf("Can't copy the key-value pair %v:%v to the destination snapshot: %v", k, v, err)

					return err
				}

				return nil
			}); err != nil {
				Log.Errorf("Can't create the snapshot DB because there was a problem writing the key-value pair to destination: %v", err)

				return err
			}

			Log.Debugf("Finished copy the entire source database to the snapshot DB")

			return nil
		}); err != nil {
			Log.Errorf("Something went wrong when trying to open the snapshot database")

			return err
		}

		return nil
	}); err != nil {
		Log.Errorf("Something went wrong when trying to open the source database")

		return err
	}

	return nil
}

func (driver *BoltDBStorageDriver) OpenSnapshot(snapshotDirectory string) (StorageDriver, error) {
	snapshotDB := NewBoltDBStorageDriver(snapshotDirectory, driver.rootBucket, driver.mode, &bolt.Options{ReadOnly: true})

	if err := snapshotDB.Open(); err != nil {
		prometheusRecordStorageError("openSnapshot()", driver.file)

		return nil, err
	}

	return snapshotDB, nil
}

func (driver *BoltDBStorageDriver) Restore(storageDriver StorageDriver) error {
	Log.Debugf("Restoring storage state from snapshot...")

	if otherDriver, ok := storageDriver.(*BoltDBStorageDriver); ok {
		err := driver.restoreBolt(otherDriver)

		if err != nil {
			prometheusRecordStorageError("restore()", driver.file)
		}

		return err
	}

	return errors.New("Snapshot source format not supported")
}

func (driver *BoltDBStorageDriver) restoreBolt(otherDriver *BoltDBStorageDriver) error {
	if err := boltCopy(driver.rootBucket, driver.db, otherDriver.db); err != nil {
		Log.Errorf("Unable to copy snapshot data to primary node storage: %v", err)

		return err
	}

	Log.Debugf("Copied snapshot data to node storage successfully")

	return nil
}
