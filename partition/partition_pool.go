package partition

type PartitionPool interface {
    Add(partition Partition)
    Remove(partitionNumber uint64)
    Get(partitionNumber uint64) Partition
}