package node

// A Node coordinates interactions between
// internal node components
type Node interface {
    // Notify this node that a site
    // has been added
    AddSite(siteID string)
    // Notify this node that a site
    // has been removed
    RemoveSite(siteID string)
    // Notify this node that a relay has
    // been added
    AddRelay(relayID string)
    // Notify this node that a relay has
    // been removed
    RemoveRelay(relayID string)
    // Notify this node that a relay has
    // been moved to another site
    MoveRelay(relayID, siteID string)
    // Notify this node that it has gained
    // ownership over a partition replica
    GainPartitionReplica(partition, replica uint64)
    // Notify this node that it has lost
    // ownership over a partition replica
    LosePartitionReplica(partition, replica uint64)
}