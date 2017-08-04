package util

import (
    . "devicedb/storage"
)

func MakeNewStorageDriver() StorageDriver {
    return NewLevelDBStorageDriver("/tmp/testdb-" + RandomString(), nil)
}