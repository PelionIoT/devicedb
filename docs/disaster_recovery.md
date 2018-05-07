**Backup**
Amazon EBS
?? Create a Kubernetes Job that performs consistent snapshot of Amazon EBS ??
For each node...

devicedb lock updates/flush changes to disk
bash: sync
perform ec2 volume snapshot
devicedb unlock updates


Maybe switch to boltdb which supports snapshots more easily


Problem with raft state and snapshots:
Maybe just use one of the instance snapshots for the "cluster state snapshot" which contains node membership, token assignments, replication settings, sites, and relays

devicedb cluster snapshot
   -seed -> include cluster state in snapshot. this is the seed snapshot


devicedb cluster restore  

**Backup**
Backs up everything as is
$ devicedb cluster snapshot -host devicedb-0.devicedb -port 8080
Write to /var/devicedb/backups/snapshot-1111
$ devicedb cluster snapshot -host devicedb-1.devicedb -port 8080
Write to /var/devicedb/backups/snapshot-1111
$ devicedb cluster snapshot -host devicedb-2.devicedb -port 8080
Write to /var/devicedb/backups/snapshot-1111

**Restore**
Create three nodes where snapshot contents are mounted under /var/devicedb/data
$ devicedb cluster start -store /var/devicedb/data -restore -host devicedb-0.devicedb
$ devicedb cluster start -store /var/devicedb/data -restore -host devicedb-1.devicedb -join devicedb-0.devicedb
$ devicedb cluster start -store /var/devicedb/data -restore -host devicedb-2.devicedb -join devicedb-0.devicedb

Cluster State
- relays
- sites
- cluster members ???
- token assignments
- holders ???

-restore flag
Nodes use the node ID stored in their snapshot
Forget raft history stored in their snapshot
Seed node
- put snapshotted cluster state as "Restore State" command in the raft log as the very first command
Other nodes
- on join, they see the Set State command and receive

Add RestoreState to cluster state which contains token assignments