package storage

import (
	. "devicedb/error"
	. "devicedb/logging"
	"errors"
	"sort"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	levelErrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDBIterator struct {
	snapshot  *leveldb.Snapshot
	it        iterator.Iterator
	ranges    []*util.Range
	prefix    []byte
	err       error
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
				it.ranges = []*util.Range{}
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
		prometheusRecordStorageError("iterator.next()", "")
		it.err = it.it.Error()
		it.ranges = []*util.Range{}
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
	it.ranges = []*util.Range{}
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
	file    string
	options *opt.Options
	db      *leveldb.DB
}

func NewLevelDBStorageDriver(file string, options *opt.Options) *LevelDBStorageDriver {
	return &LevelDBStorageDriver{file, options, nil}
}

func (levelDriver *LevelDBStorageDriver) Open() error {
	levelDriver.Close()

	db, err := leveldb.OpenFile(levelDriver.file, levelDriver.options)

	if err != nil {
		prometheusRecordStorageError("open()", levelDriver.file)

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
		prometheusRecordStorageError("recover()", levelDriver.file)

		return err
	}

	levelDriver.db = db

	return nil
}

func (levelDriver *LevelDBStorageDriver) Compact() error {
	if levelDriver.db == nil {
		return errors.New("Driver is closed")
	}

	err := levelDriver.db.CompactRange(util.Range{})

	if err != nil {
		prometheusRecordStorageError("compact()", levelDriver.file)

		return err
	}

	return nil
}

func (levelDriver *LevelDBStorageDriver) Get(keys [][]byte) ([][]byte, error) {
	if levelDriver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	if keys == nil {
		return [][]byte{}, nil
	}

	snapshot, err := levelDriver.db.GetSnapshot()

	defer snapshot.Release()

	if err != nil {
		prometheusRecordStorageError("get()", levelDriver.file)

		return nil, err
	}

	values := make([][]byte, len(keys))

	for i, key := range keys {
		if key == nil {
			values[i] = nil
		} else {
			values[i], err = snapshot.Get(key, &opt.ReadOptions{false, opt.DefaultStrict})

			if err != nil {
				if err.Error() != "leveldb: not found" {
					prometheusRecordStorageError("get()", levelDriver.file)

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

	for i := 0; i < len(s); i += 1 {
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

func (levelDriver *LevelDBStorageDriver) GetMatches(keys [][]byte) (StorageIterator, error) {
	if levelDriver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	keys = consolidateKeys(keys)
	snapshot, err := levelDriver.db.GetSnapshot()

	if err != nil {
		prometheusRecordStorageError("getMatches()", levelDriver.file)

		snapshot.Release()

		return nil, err
	}

	ranges := make([]*util.Range, 0, len(keys))

	if keys == nil {
		return &LevelDBIterator{snapshot, nil, ranges, nil, nil, FORWARD}, nil
	}

	for _, key := range keys {
		if key == nil {
			continue
		} else {
			ranges = append(ranges, util.BytesPrefix(key))
		}
	}

	return &LevelDBIterator{snapshot, nil, ranges, nil, nil, FORWARD}, nil
}

func (levelDriver *LevelDBStorageDriver) GetRange(min, max []byte) (StorageIterator, error) {
	if levelDriver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	snapshot, err := levelDriver.db.GetSnapshot()

	if err != nil {
		prometheusRecordStorageError("getRange()", levelDriver.file)

		snapshot.Release()

		return nil, err
	}

	ranges := []*util.Range{&util.Range{min, max}}

	return &LevelDBIterator{snapshot, nil, ranges, nil, nil, FORWARD}, nil
}

func (levelDriver *LevelDBStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	if levelDriver.db == nil {
		return nil, errors.New("Driver is closed")
	}

	snapshot, err := levelDriver.db.GetSnapshot()

	if err != nil {
		prometheusRecordStorageError("getRanges()", levelDriver.file)

		snapshot.Release()

		return nil, err
	}

	var levelRanges = make([]*util.Range, len(ranges))

	for i := 0; i < len(ranges); i += 1 {
		levelRanges[i] = &util.Range{ranges[i][0], ranges[i][1]}
	}

	return &LevelDBIterator{snapshot, nil, levelRanges, nil, nil, direction}, nil
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

	err := levelDriver.db.Write(b, nil)

	if err != nil {
		prometheusRecordStorageError("batch()", levelDriver.file)
	}

	return err
}

func (levelDriver *LevelDBStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	if levelDriver.db == nil {
		return errors.New("Driver is closed")
	}

	snapshotDB, err := leveldb.OpenFile(snapshotDirectory, &opt.Options{})

	if err != nil {
		prometheusRecordStorageError("snapshot()", levelDriver.file)

		Log.Errorf("Can't create snapshot because %s could not be opened for writing: %v", snapshotDirectory, err)

		return err
	}

	Log.Debugf("Copying database contents to snapshot at %s", snapshotDirectory)

	if err := levelCopy(snapshotDB, levelDriver.db); err != nil {
		prometheusRecordStorageError("snapshot()", levelDriver.file)

		Log.Errorf("Can't create snapshot because there was an error while copying the keys: %v", err)

		return err
	}

	var metaBatch *leveldb.Batch = &leveldb.Batch{}

	Log.Debugf("Recording snapshot metadata: %v", metadata)

	// Now write the snapshot metadata
	for metaKey, metaValue := range metadata {
		var key []byte = make([]byte, len(metadataPrefix)+len([]byte(metaKey)))

		copy(key, metadataPrefix)
		copy(key[len(metadataPrefix):], []byte(metaKey))

		metaBatch.Put(key, []byte(metaValue))
	}

	if err := snapshotDB.Write(metaBatch, &opt.WriteOptions{Sync: true}); err != nil {
		prometheusRecordStorageError("snapshot()", levelDriver.file)

		Log.Errorf("Can't create snapshot because there was a problem recording the snapshot metadata: %v", err)

		return err
	}

	if err := snapshotDB.Close(); err != nil {
		prometheusRecordStorageError("snapshot()", levelDriver.file)

		Log.Errorf("Can't create snapshot because there was an error while closing the snapshot database at %s: %v", snapshotDirectory, err)

		return err
	}

	Log.Debugf("Created snapshot at %s", snapshotDirectory)

	return nil
}

func levelCopy(dest *leveldb.DB, src *leveldb.DB) error {
	iter := src.NewIterator(&util.Range{}, &opt.ReadOptions{DontFillCache: true})

	defer iter.Release()

	var batch *leveldb.Batch = &leveldb.Batch{}
	var batchSizeBytes int
	var totalKeys uint64

	for iter.Next() {
		totalKeys++
		batch.Put(iter.Key(), iter.Value())
		batchSizeBytes += len(iter.Key()) + len(iter.Value())

		if batchSizeBytes >= CopyBatchMaxBytes || batch.Len() >= CopyBatchSize {
			Log.Debugf("Writing next copy chunk (batch.Len() = %d, batchSizeBytes = %d, totalKeys = %d)", batch.Len(), batchSizeBytes, totalKeys)

			if err := dest.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
				Log.Errorf("Can't create copy because there was a problem writing the next chunk to destination: %v", err)

				return err
			}

			batchSizeBytes = 0
			batch.Reset()
		}
	}

	if iter.Error() != nil {
		Log.Errorf("Can't create copy because there was an iterator error: %v", iter.Error())

		return iter.Error()
	}

	// Write the rest of the records in one last batch
	if batch.Len() > 0 {
		Log.Debugf("Writing next copy chunk (batch.Len() = %d, batchSizeBytes = %d, totalKeys = %d)", batch.Len(), batchSizeBytes, totalKeys)

		if err := dest.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
			Log.Errorf("Can't create copy because there was a problem writing the next chunk to destination: %v", err)

			return err
		}
	}

	return nil
}

func (levelDriver *LevelDBStorageDriver) OpenSnapshot(snapshotDirectory string) (StorageDriver, error) {
	snapshotDB := NewLevelDBStorageDriver(snapshotDirectory, &opt.Options{ErrorIfMissing: true, ReadOnly: true})

	if err := snapshotDB.Open(); err != nil {
		prometheusRecordStorageError("openSnapshot()", snapshotDirectory)

		return nil, err
	}

	return snapshotDB, nil
}

func (levelDriver *LevelDBStorageDriver) Restore(storageDriver StorageDriver) error {
	Log.Debugf("Restoring storage state from snapshot...")

	if otherLevelDriver, ok := storageDriver.(*LevelDBStorageDriver); ok {
		err := levelDriver.restoreLevel(otherLevelDriver)

		if err != nil {
			prometheusRecordStorageError("restore()", levelDriver.file)
		}

		return err
	}

	return errors.New("Snapshot source format not supported")
}

func (levelDriver *LevelDBStorageDriver) restoreLevel(otherLevelDriver *LevelDBStorageDriver) error {
	if err := levelCopy(levelDriver.db, otherLevelDriver.db); err != nil {
		Log.Errorf("Unable to copy snapshot data to primary node storage: %v", err)

		return err
	}

	Log.Debugf("Copied snapshot data to node storage successfully")

	return nil
}
