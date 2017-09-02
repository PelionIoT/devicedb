package partition

import (
    . "devicedb/site"
)

// Gains ownership over
// partition i replica j
// partition.
// create PartitionTransfer
// [Initiator] ----please transfer partition i replica j to me------------> [Holder]
//                                                                 transfer = StartTransfer(nodeID)
// [Initiator] <---ok here is your transfer session id--------------------- [Holder]
// [Initiator] ----give me chunk 0 here is my session id------------------> [Holder]
// [Initiator] ----here is chunk 0----------------------------------------> [Holder]
// [Initiator] ----give me chunk 1 here is my session id------------------> [Holder]
// [Initiator] <---here is chunk 1----------------------------------------- [Holder]
//                          .............................
// [Initiator] ----give me chunk n here is my session id------------------> [Holder]
// [Initiator] <---here is chunk n----------------------------------------- [Holder]
// Chunk n is empty
// We have received
// all chunks

// receiver gains ownership over (i,j)
// Is this too literal??
// node.Decommission()
// node.AddPartitionReplica(i, j)
// node.RemovePartitionReplica(i, j)
// node.AddSite(siteID string)
// node.RemoveSite(siteID string)
// node.AddRelay(relayID string)
// node.RemoveRelay(relayID string)
// node.MoveRelay(relayID string, siteID string)
// 
// receiver loses ownership over (i,j)
// node.DownloadPartitionReplica(i, j)
// transferCoordinator.CancelTransfer(i, j)
// 
// sender 


type PartitionReplica interface {
    // Which replica number is this
    Partition() uint64
    Replica() uint64
    Sites() SitePool
    IsLocked()
    LockWrites()
    UnlockWrites()
    LockReads()
    UnlockReads()
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
