package storage

type PrefixedStorageDriver struct {
	prefix        []byte
	storageDriver StorageDriver
}

func NewPrefixedStorageDriver(prefix []byte, storageDriver StorageDriver) *PrefixedStorageDriver {
	return &PrefixedStorageDriver{prefix, storageDriver}
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
	result := make([]byte, 0, len(psd.prefix)+len(k))

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

	return &PrefixedIterator{psd.prefix, iter}, nil
}

func (psd *PrefixedStorageDriver) GetRange(start []byte, end []byte) (StorageIterator, error) {
	iter, err := psd.storageDriver.GetRange(psd.addPrefix(start), psd.addPrefix(end))

	if err != nil {
		return nil, err
	}

	return &PrefixedIterator{psd.prefix, iter}, nil
}

func (psd *PrefixedStorageDriver) GetRanges(ranges [][2][]byte, direction int) (StorageIterator, error) {
	var prefixedRanges = make([][2][]byte, len(ranges))

	for i := 0; i < len(ranges); i += 1 {
		prefixedRanges[i] = [2][]byte{psd.addPrefix(ranges[i][0]), psd.addPrefix(ranges[i][1])}
	}

	iter, err := psd.storageDriver.GetRanges(prefixedRanges, direction)

	if err != nil {
		return nil, err
	}

	return &PrefixedIterator{psd.prefix, iter}, nil
}

func (psd *PrefixedStorageDriver) Batch(batch *Batch) error {
	newBatch := NewBatch()

	for key, op := range batch.BatchOps {
		op.OpKey = psd.addPrefix([]byte(key))
		newBatch.BatchOps[string(psd.addPrefix([]byte(key)))] = op
	}

	return psd.storageDriver.Batch(newBatch)
}

func (psd *PrefixedStorageDriver) Snapshot(snapshotDirectory string, metadataPrefix []byte, metadata map[string]string) error {
	return psd.storageDriver.Snapshot(snapshotDirectory, metadataPrefix, metadata)
}

func (psd *PrefixedStorageDriver) OpenSnapshot(snapshotDirectory string) (StorageDriver, error) {
	return psd.storageDriver.OpenSnapshot(snapshotDirectory)
}

func (psd *PrefixedStorageDriver) Restore(storageDriver StorageDriver) error {
	return psd.storageDriver.Restore(storageDriver)
}

type PrefixedIterator struct {
	prefix   []byte
	iterator StorageIterator
}

func NewPrefixedIterator(iter StorageIterator, prefix []byte) *PrefixedIterator {
	return &PrefixedIterator{prefix, iter}
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
