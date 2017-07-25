package cloud

const (
	RolePrimary = iota
	RoleSecondary = iota
)

// How does logging work with partitions + sites to maximize concurrency?
// One log per site or one log per partition?
// Would one log per partition force updates to be serialized? i.e. couldn't call Batch on storage driver until the last one finished?
// Need log entries to be atomic with update?
//   if log is written but not batch then the update will be replicated to backups but not be present on primary (may be worked around if the updates are idempotent)
//   if log is not written but batch is then the update will be performed on primary but not backups
// It is important if multiple backup nodes exist for a partition that if the primary goes down and a new node needs to become the primary node for that partition that
//   it restores the partition from the backup that is most up to date. If primary is less up to date than a secondary node for that partition what happens?
//   Hmmmm why not just make finding out which node is the most up to date for a partition part of the restructuring process
// If I allow site-level logging to improve parallelism per partition then it would mean some sites would be more up to date on some backup nodes. There would be no
// de-facto "latest" unless I enforced the order that backups get written (ranked backup nodes). However, this would result in slower backup convergence since they
// must be done serially. Is there some way to allow backups to diverge in this way and then enforce convergence when moving the primary or promoting a backup to the primary?
// Backup convergence algorithm?
// Log structure:
//   site 1:
//     op 1
//     op 2
//     ...
//   site 2:
//     op 2
//     op 3
//     ...
//   site 3:
// Log digest: { site1: 5, site2: 8, site3: 2 } // maps site id to the latest op received for that site
// If I've been elected as primary
//   For each site in the digest is there some other replica that has newer ops for this site?
//   If so get the ops until i have the latest ops on every site. Propose something to the log to indicate
// If I've been elected as secondary
//   Wait until the primary is done performing log convergence for this partition then wait for it to copy over
//   the latest data for that partition
// We can delete entries from the secondary's log after the primary tells us to
// We can delete entries from the primary's log after we have performed the operation on all replica logs and then told all replicas to delete the operation from their log
// After partition update lock
//   1) CONVERGE PARTITION REPLICAS
//   2) TRANSFER PARTITIONS
//   3) TRANSFER NODE RESPONSIBILITY
// Instruction Types
//   Request Ring Update Lock (serialize bootstrapping/resizing/removing operations)
//   Release Ring Update Lock
//   Request Partition Update Lock
//   Notify Partition Locked
//   
// Update Lock holder - 
//   Propose 
// 
