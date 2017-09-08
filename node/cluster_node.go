package node

import (
    "sync"

    . "devicedb/cluster"
)

type ClusterNode struct {
    ConfigController *ConfigController
    // PartitionReplicaPool contains a PartitionReplica
    // for all partition replicas held by this node
    Partitions PartitionPool
    PartitionFactory PartitionFactory
    lock sync.Mutex
}

func New(configController *ConfigController, partitions PartitionRegistry, partitionFactory PartitionFactory) *ClusterNode {
    clusterNode := &ClusterNode{
        Partitions: partitions,
        ConfigController: configController,
        PartitionFactory: partitionFactory,
    }

    clusterNode.initialize()

    return clusterNode
}

func (node *ClusterNode) initialize() {
    node.initializePartitions()
}

func (node *ClusterNode) initializePartitions() {
    ownedPartitions := node.ConfigController.ClusterController().LocalNodeOwnedPartitions()
    heldPartitions := node.ConfigController.ClusterController().LocalNodeHeldPartitions()

    for _, partitionNumber := range heldPartitions {
        partition := node.PartitionFactory.CreatePartition(partitionNumber)

        if !node.ConfigController.ClusterController().LocalNodeOwnsPartition() {
            // partitions that are held by this node but not owned anymore by this node
            // should be write locked but should still allow reads so that the new
            // owner(s) can transfer the partition data
            partition.LockWrites()
        }

        node.Partitions.Register(partitionNumber, partition)
    }

    for _, partitionNumber := range ownedPartitions {
        if !node.Partitions.Has(partitionNumber) {
            // partitions that are owned by this node but for which this node
            // has not yet received a full snapshot from an old owner should
            // be read locked until the transfer is complete but still allow
            // writes to its state can remain up to date if updates happen
            // concurrently with the range transfer. However, such writes do
            // not count toward the write quorum
            partition := node.PartitionFactory.CreatePartition(partitionNumber)
            partition.LockReads()
            node.Partitions.Register(partitionNumber, partition)
            // start download initiates the transfer from the old owner or
            // another replica which contains the needed data
            partition.StartDownload()
        }
    }
}

func (node *ClusterNode) AddSite(siteID string) {
    node.lock.Lock()
    defer node.lock.Unlock()

    sitePartition := node.ConfigController.ClusterController().Partition(siteID)
    partition := node.PartitionPool.Get(sitePartition)

    // A nil partition indicates that we do not own any replica of this partition
    if partition == nil {
        return
    }

    partition.Sites().Add(siteID)
}

func (node *ClusterNode) RemoveSite(siteID string) {
    node.lock.Lock()
    defer node.lock.Unlock()

    sitePartition := node.ConfigController.ClusterController().Partition(siteID)
    partition := node.PartitionPool.Get(sitePartition)

    if partition == nil {
        return
    }

    partition.Sites().Remove(siteID)
}

func (node *ClusterNode) AddRelay(relayID string) {
    // Nothing to do
}

func (node *ClusterNode) RemoveRelay(relayID string) {
    // Disconnect the relay if it's currently connected
}

func (node *ClusterNode) MoveRelay(relayID, siteID string) {
    // Disconnect the relay if it's currently connected
}

func (node *ClusterNode) AcceptRelayConnection() {
    // TODO
}

// We need to have some "partition replica" entity around as long as we are a holder
// only unlocked if we own and hold the partition replica
// What if we own all replicas of a partition and another node comes online and needs
// us to transfer data to it? Should we lock that partition?
// n = # nodes in the cluster
// r = replication factor
// This is a problem as long as n < r
// Case 1) I am losing ownership over a partition replica but retain ownership over another replica of that partition:
//     - n < r and a node is being added to the cluster
// Case 2) I am losing ownership over a partition replica and it is the only replica of that partition that i own:
//     - n >= r and a node is being added
// Lock if and only if I do not have ownership over any replicas of this partition
// While transfer is occurring quorum will fail
// 
// Transfers:
//   I can accept writes as soon as I become partition replica owner
//   but can only accept reads once I finish a partition transfer from
//   some other replica. Accept writes while simultaneously bootstrapping
//   I cannot perform a range transfer to another node if i myself
//   have not yet finished accepting a range transfer for that partition
//   
//   Case: Two nodes are added simultaneously (n2 and n3) that both own a replica
//   of some partition and currently only one node (n1) holds a replica for
//   that partition. n2 and n3 both ask n1 to transfer its contents to them for
//   that partition since it is the only node that holds the partition
//
// ...
// Node Partition Replica bootstrapping:
//   A node is bootstrapping as long as it owns a partition replica but 
//   has not completed a data transfer for that partition from an existing
//   replica holder.
//   Writes can be forwarded to a node while it is bootstrapping but writes
//   to it do not contribute to write quorum.
//   A node cannot serve reads when it is bootstrapping. It must have succesfully
//   announced that it holds that partition replica through the raft log
//   first
//   Choosing a transfer partner:
//     The node that needs to perform a partition transfer prioritizes transfer
//     partners like so from best candidate to worst:
//       1) A node that is a holder of a replica that this node now owns
//       2) A node that is a holder of some replica of this partition but not one that overlaps with us
//     If no holders exist for this partition then skip to the end where this node becomes a holder for
//     those partitions that it owns

func (node *ClusterNode) UpdatePartition(partitionNumber uint64) {
    node.lock.Lock()
    defer node.lock.Unlock()

    nodeHoldsPartition := node.ConfigController.ClusterController().LocalNodeHoldsPartition(partitionNumber)
    nodeOwnsPartition := node.ConfigController.ClusterController().LocalNodeOwnsPartition(partitionNumber)
    partition := node.Partitions.Get(partitionNumber)

    if !nodeOwnsPartition && !nodeHoldsPartition {
        if partition != nil {
            partition.Teardown()
            node.Partitions.Unregister(partitionNumber)
        }

        return
    }

    if partition == nil {
        partition = node.PartitionFactory.CreatePartition(partitionNumber)
    }

    if nodeOwnsPartition {
        partition.UnlockWrites()
    } else {
        partition.LockWrites()
    }

    if nodeHoldsPartition {
        // allow reads so this partition data can be transferred to another node
        // or this node can serve reads for this partition
        partition.UnlockReads()
    } else {
        // lock reads until this node has finalized a partition transfer
        partition.LockReads()
    }

    if nodeOwnsPartition {
        if !nodeHoldsPartition {
            // Start transfer if it is not already in progress for this partition
            if !partition.TransferIncoming() {
                partition.StartTransfer()
            }
        } else {
            if partition.TransferIncoming() {
                partition.FinishTransfer()
            }
        }
    }

    if node.Partitions.Get(partitionNumber) == nil {
        node.Partitions.Register(partition)
    }
}
