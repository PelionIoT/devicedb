package partition

import (
    . "devicedb/site"
)

type Partition interface {
    Partition() uint64
    Replica() uint64
    Sites() SitePool
    Iterator() PartitionIterator
    LockWrites()
    UnlockWrites()
    LockReads()
    UnlockReads()
}
