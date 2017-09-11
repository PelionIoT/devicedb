package partition

type PartitionFactory interface {
	CreatePartition(partitionNumber uint64) Partition
}