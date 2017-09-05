package partition

import (
    . "devicedb/site"
    . "devicedb/storage"
)

type PartitionReplica interface {
    Partition() uint64
    Replica() uint64
    Sites() SitePool
    Iterator() PartitionIterator
    IsLocked()
    LockWrites()
    UnlockWrites()
    LockReads()
    UnlockReads()
}
