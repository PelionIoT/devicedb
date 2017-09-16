package node

type ClusterNodeCoordinatorFacade interface {
    ID() uint64
    // Create, initialize, and add a partition to the node's
    // partition pool if it isn't already there. Initializes the partition as both
    // read and write locked
    AddPartition(partitionNumber uint64)
    // Remove a partition from the node's partition pool.
    RemovePartition(partitionNumber uint64)
    // Allow other nodes to request a copy of this partition's
    // data
    EnableOutgoingTransfers(partitionNumber uint64)
    // Disallow other nodes from requesting a copy of this partition's
    // data. Cancel any current outgoing transfers for this partition
    DisableOutgoingTransfers(partitionNumber uint64)
    // Obtain a copy of this partition's data if necessary from another
    // node and then transfer holdership of this partition replica
    // to this node
    StartIncomingTransfer(partitionNumber uint64, replicaNumber uint64)
    // Stop any pending or ongoing transfers of this partition replica to
    // this node. Also cancel any downloads for this partition from 
    // any other replicas
    StopIncomingTransfer(partitionNumber uint64, replicaNumber uint64)
    // Ensure that no updates can occur to the local copy of this partition
    LockPartitionWrites(partitionNumber uint64)
    // Allow updates to the local copy of this partition
    UnlockPartitionWrites(partitionNumber uint64)
    // Ensure that the local copy of this partition cannot be read for
    // transfers or any other purpose
    LockPartitionReads(partitionNumber uint64)
    // Allow the local copy of this partition to serve reads
    UnlockPartitionReads(partitionNumber uint64)
    // Add site to the partition that it belongs to
    // if this node owns that partition
    AddSite(siteID string)
    // Remove site from the partition that it belongs to
    // if this node owns that partition. Disconnect any
    // relays that are in that site
    RemoveSite(siteID string)
    AddRelay(relayID string)
    RemoveRelay(relayID string)
    MoveRelay(relayID string, siteID string)
    // Obtain a two dimensional map indicating which partition replicas are
    // currently owned by this node. map[partitionNumber][replicaNumber]
    OwnedPartitionReplicas() map[uint64]map[uint64]bool
    // Obtain a two dimensional map indicating which partition replicas are
    // currently held by this node. map[partitionNumber][replicaNumber]
    HeldPartitionReplicas() map[uint64]map[uint64]bool
    // Notify a node that it has been added to a cluster
    NotifyJoinedCluster()
    // Notify a node that it has been removed from a cluster
    NotifyLeftCluster()
    // Notify a node that it no longer owns or holds any partition replicas
    NotifyEmpty()
}

type NodeCoordinatorFacade struct {
    node *ClusterNode
}

func (nodeFacade *NodeCoordinatorFacade) ID() uint64 {
    return nodeFacade.node.ID()
}

func (nodeFacade *NodeCoordinatorFacade) AddPartition(partitionNumber uint64) {
    if nodeFacade.node.partitionPool.Get(partitionNumber) == nil {
        partition := nodeFacade.node.partitionFactory.CreatePartition(partitionNumber, nodeFacade.node.sitePool(partitionNumber))
        partition.LockReads()
        partition.LockWrites()
        nodeFacade.node.partitionPool.Add(partition)
    }
}

func (nodeFacade *NodeCoordinatorFacade) RemovePartition(partitionNumber uint64) {
    nodeFacade.node.partitionPool.Remove(partitionNumber)
}

func (nodeFacade *NodeCoordinatorFacade) EnableOutgoingTransfers(partitionNumber uint64) {
    nodeFacade.node.transferAgent.EnableOutgoingTransfers(partitionNumber)
}

func (nodeFacade *NodeCoordinatorFacade) DisableOutgoingTransfers(partitionNumber uint64) {
    nodeFacade.node.transferAgent.DisableOutgoingTransfers(partitionNumber)
}

func (nodeFacade *NodeCoordinatorFacade) StartIncomingTransfer(partitionNumber uint64, replicaNumber uint64) {
    nodeFacade.node.transferAgent.StartTransfer(partitionNumber, replicaNumber)
}

func (nodeFacade *NodeCoordinatorFacade) StopIncomingTransfer(partitionNumber uint64, replicaNumber uint64) {
    nodeFacade.node.transferAgent.StopTransfer(partitionNumber, replicaNumber)
}

func (nodeFacade *NodeCoordinatorFacade) LockPartitionWrites(partitionNumber uint64) {
    partition := nodeFacade.node.partitionPool.Get(partitionNumber)

    if partition != nil {
        partition.LockWrites()
    }
}

func (nodeFacade *NodeCoordinatorFacade) UnlockPartitionWrites(partitionNumber uint64) {
    partition := nodeFacade.node.partitionPool.Get(partitionNumber)

    if partition != nil {
        partition.UnlockWrites()
    }
}

func (nodeFacade *NodeCoordinatorFacade) LockPartitionReads(partitionNumber uint64) {
    partition := nodeFacade.node.partitionPool.Get(partitionNumber)

    if partition != nil {
        partition.LockReads()
    }
}

func (nodeFacade *NodeCoordinatorFacade) UnlockPartitionReads(partitionNumber uint64) {
    partition := nodeFacade.node.partitionPool.Get(partitionNumber)

    if partition != nil {
        partition.UnlockReads()
    }
}

func (nodeFacade *NodeCoordinatorFacade) AddSite(siteID string) {
}

func (nodeFacade *NodeCoordinatorFacade) RemoveSite(siteID string) {
}

func (nodeFacade *NodeCoordinatorFacade) AddRelay(relayID string) {
}

func (nodeFacade *NodeCoordinatorFacade) RemoveRelay(relayID string) {
}

func (nodeFacade *NodeCoordinatorFacade) MoveRelay(relayID string, siteID string) {
}

func (nodeFacade *NodeCoordinatorFacade) OwnedPartitionReplicas() map[uint64]map[uint64]bool {
    var ownedPartitionReplicas map[uint64]map[uint64]bool = make(map[uint64]map[uint64]bool, 0)

    for _, partitionReplica := range nodeFacade.node.configController.ClusterController().LocalNodeOwnedPartitionReplicas() {
        if _, ok := ownedPartitionReplicas[partitionReplica.Partition]; !ok {
            ownedPartitionReplicas[partitionReplica.Partition] = make(map[uint64]bool)
        }

        ownedPartitionReplicas[partitionReplica.Partition][partitionReplica.Replica] = true
    }

    return ownedPartitionReplicas
}

func (nodeFacade *NodeCoordinatorFacade) HeldPartitionReplicas() map[uint64]map[uint64]bool {
    var heldPartitionReplicas map[uint64]map[uint64]bool = make(map[uint64]map[uint64]bool, 0)

    for _, partitionReplica := range nodeFacade.node.configController.ClusterController().LocalNodeHeldPartitionReplicas() {
        if _, ok := heldPartitionReplicas[partitionReplica.Partition]; !ok {
            heldPartitionReplicas[partitionReplica.Partition] = make(map[uint64]bool)
        }

        heldPartitionReplicas[partitionReplica.Partition][partitionReplica.Replica] = true
    }

    return heldPartitionReplicas
}

func (nodeFacade *NodeCoordinatorFacade) NotifyJoinedCluster() {
    nodeFacade.node.joinedCluster <- 1
}

func (nodeFacade *NodeCoordinatorFacade) NotifyLeftCluster() {
    nodeFacade.node.leftCluster <- 1
}
    
func (nodeFacade *NodeCoordinatorFacade) NotifyEmpty() {
    nodeFacade.node.empty <- 1
}