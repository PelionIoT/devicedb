package storage

import (
	. "devicedb/logging"
	"errors"

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

func (iter *BoltDBStorageIterator) Next() bool {
	var tx *bolt.Tx
	var err error

	if tx, err = iter.db.Begin(false); err != nil {
		prometheusRecordStorageError("iterator.next()", "")
		iter.err = err

		return false
	}
	defer tx.Rollback()

	var b *bolt.Bucket
	if b = tx.Bucket([]byte(iter.rootBucket)); b == nil {
		prometheusRecordStorageError("iterator.next()", "")
		iter.err = errors.New("there is no such bucket in BoltDB")

		return false
	}

	c := b.Cursor()

	if iter.value == nil {
		if len(iter.ranges) == 0 {
			return false
		}

		iter.prefix = iter.ranges[0].start
		iter.ranges = iter.ranges[1:]

		if iter.direction == BACKWARD {
			k, _ := c.Last()
			if string(iter.prefix) == string(k) {
				return true
			}

			iter.Release()

			return false
		}
	}

	cur_k, cur_v := c.Seek(key)

	if iter.direction == BACKWARD {
		if cur_k != nil && string(cur_k) != string(iter.prefix) {
			prev_k, prev_v = c.Prev()
			iter.key = prev_k
			iter.value = prev_v

			if prev_k != nil {
				return true
			}
		}

	} else {
		if cur_k != nil && string(cur_k) != string(iter.ranges[0].end) {
			next_k, next_v = c.Next()
			iter.key = next_k
			iter.value = next_v

			if next_k != nil {
				return true
			}
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
}

func (iter *BoltDBStorageIterator) Error() error {
	return iter.err
}

type BoltDBStorageDriver struct {
	file       string
	rootBucket string
	options    *bolt.Options
	db         *bolt.DB
}

func NewBoltDBStorageDriver(file string, rootBucket string, options *bolt.Options) *BoltDBStorageDriver {
	return &BoltDBStorageDriver{file, rootBucket, options, nil}
}

func (driver *BoltDBStorageDriver) Open() error {
	driver.Close()

	db, err := bolt.Open(driver.file, 0666, driver.options)
	if err != nil {
		Log.Errorf("Could not open the database: %v. Error: %v", driver.file, err)

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

	if err := driver.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(driver.rootBucket))
		if bucket == nil {
			Log.Errorf("The root bucket is not exist")

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
		Log.Errorf("Something went wrong when trying to initialize the transaction of the database")

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
		return nil, errors.New("Drives is closed")
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
				Log.Errorf("Something went wrong when trying to create a bucket: %v", err)

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
		Log.Errorf("Something went wrong when trying to insert or delete data")

		return err
	}

	return nil
}

func (driver *BoltDBStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	if driver.db == nil {
		return errors.New("Driver is closed")
	}

	snapshotDB, err := bolt.Open(snapshotDirectory, 0666, nil)
	if err != nil {
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

			Log.Error("Can't find the root bucket in the snapshot")

			return errors.New("Can't create snapshot because there was a problem opening the root bucket in the snapshot")
		}

		Log.Debugf("Recording snapshot metadata: %v", metadata)

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

		Log.Errorf("Can't create the snapshot because there was a problem opening the snapshot")

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
					Log.Errorf("Can't create the root bucket in the snapshot DB when trying to copy the key-value pair to snapshot DB")

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
	snapshotDB := NewBoltDBStorageDriver(snapshotDirectory, driver.rootBucket, nil)

	if err := snapshotDB.Open(); err != nil {
		prometheusRecordStorageError("restore()", driver.file)

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
