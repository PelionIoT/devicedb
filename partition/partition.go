package partition

import (
    . "devicedb/site"
)

type Partition interface {
    Sites() SitePool
    Lock()
    Unlock()
}

type BasicPartition struct {
    SitePool SitePool
}

func (partition *BasicPartition) Sites() SitePool {
    return partition.SitePool
}

func (partition *BasicPartition) Lock() {
}

func (partition *BasicPartition) Unlock() {
}
