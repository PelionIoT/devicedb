package node

import (
    "context"
    "sync"

    . "devicedb/cluster"
    . "devicedb/error"
    . "devicedb/logging"
    . "devicedb/partition"
    . "devicedb/raft"
    . "devicedb/server"
    . "devicedb/storage"
    . "devicedb/transfer"
    . "devicedb/util"
)

type ClusterNodeConfig struct {
    StorageDriver StorageDriver
    CloudServer *CloudServer
}

type ClusterNode struct {
    configController ClusterConfigController
    configControllerBuilder ClusterConfigControllerBuilder
    cloudServer *CloudServer
    raftTransport *TransportHub
    raftStore RaftNodeStorage
    transferAgent PartitionTransferAgent
    storageDriver StorageDriver
    partitionFactory PartitionFactory
    partitionPool PartitionPool
    joinedCluster chan int
    leftCluster chan int
    shutdown chan int
    lock sync.Mutex
}

func New(config ClusterNodeConfig) *ClusterNode {
    clusterNode := &ClusterNode{
        shutdown: make(chan int),
        storageDriver: config.StorageDriver,
        cloudServer: config.CloudServer,
    }

    return clusterNode
}

func (node *ClusterNode) UseConfigControllerBuilder(configControllerBuilder ClusterConfigControllerBuilder) {
    node.configControllerBuilder = configControllerBuilder
}

func (node *ClusterNode) UseRaftStore(raftStore RaftNodeStorage) {
    node.raftStore = raftStore
}

/*func (node *ClusterNode) UpdatePartition(partitionNumber uint64) {
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
        node.PartitionTransferAgent.DisableOutgoingTransfers(partitionNumber)
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
}*/

func (node *ClusterNode) getNodeID() (uint64, error) {
    if err := node.raftStore.Open(); err != nil {
        Log.Criticalf("Local node unable to open raft store: %v", err.Error())

        return 0, err
    }

    nodeID, err := node.raftStore.NodeID()

    if err != nil {
        Log.Criticalf("Local node unable to obtain node ID from raft store: %v", err.Error())

        return 0, err
    }

    if nodeID == 0 {
        nodeID = UUID64()

        Log.Infof("Local node initializing with ID %d", nodeID)

        if err := node.raftStore.SetNodeID(nodeID); err != nil {
            Log.Criticalf("Local node unable to store new node ID: %v", err.Error())

            return 0, err
        }
    }

    return nodeID, nil
}

func (node *ClusterNode) Start(options NodeInitializationOptions) error {
    if err := node.openStorageDriver(); err != nil {
        return err
    }

    nodeID, err := node.getNodeID()

    if err != nil {
        return err
    }

    Log.Infof("Local node (id = %d) starting up...", nodeID)

    clusterHost, clusterPort := options.ClusterAddress()
    node.configControllerBuilder.SetLocalNodeAddress(PeerAddress{ NodeID: nodeID, Host: clusterHost, Port: clusterPort })
    node.configControllerBuilder.SetRaftNodeStorage(node.raftStore)
    node.configControllerBuilder.SetRaftNodeTransport(nil)
    node.configControllerBuilder.SetCreateNewCluster(options.ShouldStartCluster())
    node.configController = node.configControllerBuilder.Create()

    node.configController.OnLocalUpdates(func(deltas []ClusterStateDelta) {
        node.ProcessClusterUpdates(deltas)
    })

    node.configController.Start()
    defer node.stop()

    if err := node.startNetworking(); err != nil {
        return err
    }

    if node.configController.ClusterController().LocalNodeWasRemovedFromCluster() {
        Log.Errorf("Local node (id = %d) unable to start because it was removed from the cluster", nodeID)

        return ERemoved
    }

    decommission, err := node.raftStore.IsDecommissioning()

    if err != nil {
        Log.Criticalf("Local node (id = %d) unable to start up since it could not check the decomissioning flag: %v", nodeID, err.Error())

        return err
    }

    if decommission {
        Log.Infof("Local node (id = %d) will resume decommissioning process", nodeID)

        return node.LeaveCluster()
    }

    if !node.configController.ClusterController().LocalNodeIsInCluster() {
        if options.ShouldJoinCluster() {
            seedHost, seedPort := options.SeedNode()

            Log.Infof("Local node (id = %d) joining existing cluster. Seed node at %s:%d", nodeID, seedHost, seedPort)

            if err := node.JoinCluster(seedHost, seedPort); err != nil {
                Log.Criticalf("Local node (id = %d) unable to join cluster: %v", nodeID, err.Error())

                return err
            }
        } else {
            Log.Infof("Local node (id = %d) creating new cluster...", nodeID)

            if err := node.InitializeCluster(options.ClusterSettings); err != nil {
                Log.Criticalf("Local node (id = %d) unable to create new cluster: %v", nodeID, err.Error())

                return err
            }
        }
    }

    return nil
}

func (node *ClusterNode) openStorageDriver() error {
    if err := node.storageDriver.Open(); err != nil {
        if err != ECorrupted {
            Log.Criticalf("Error opening storage driver: %v", err.Error())
            
            return EStorage
        }

        Log.Error("Database is corrupted. Attempting automatic recovery now...")

        recoverError := node.recover()

        if recoverError != nil {
            Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())
            Log.Critical("Database daemon will now exit")

            return EStorage
        }
    }

    return nil
}

func (node *ClusterNode) recover() error {
    recoverError := node.storageDriver.Recover()

    if recoverError != nil {
        Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())

        return EStorage
    }

    return nil
}

func (node *ClusterNode) initializePartitions() {
    var ownedPartitionReplicas map[uint64]map[uint64]bool
    var heldPartitionReplicas map[uint64]map[uint64]bool

    for _, partitionReplica := range node.configController.ClusterController().LocalNodeOwnedPartitionReplicas() {
        if _, ok := ownedPartitionReplicas[partitionReplica.Partition]; !ok {
            ownedPartitionReplicas[partitionReplica.Partition] = make(map[uint64]bool)
        }

        ownedPartitionReplicas[partitionReplica.Partition][partitionReplica.Replica] = true
    }

    for _, partitionReplica := range node.configController.ClusterController().LocalNodeHeldPartitionReplicas() {
        if _, ok := heldPartitionReplicas[partitionReplica.Partition]; !ok {
            heldPartitionReplicas[partitionReplica.Partition] = make(map[uint64]bool)
        }

        heldPartitionReplicas[partitionReplica.Partition][partitionReplica.Replica] = true
    }

    for partitionNumber, _ := range heldPartitionReplicas {
        partition := node.partitionFactory.CreatePartition(partitionNumber)

        if len(ownedPartitionReplicas[partitionNumber]) == 0 {
            // partitions that are held by this node but not owned anymore by this node
            // should be write locked but should still allow reads so that the new
            // owner(s) can transfer the partition data
            partition.LockWrites()
        }

        node.partitionPool.Add(partition)
        node.transferAgent.EnableOutgoingTransfers(partitionNumber)
    }

    for partitionNumber, replicas := range ownedPartitionReplicas {
        partition := node.partitionFactory.CreatePartition(partitionNumber)

        if len(heldPartitionReplicas[partitionNumber]) == 0 {
            // partitions that are owned by this node but for which this node
            // has not yet received a full snapshot from an old owner should
            // be read locked until the transfer is complete but still allow
            // writes to its state can remain up to date if updates happen
            // concurrently with the range transfer. However, such writes do
            // not count toward the write quorum
            partition.LockReads()
        }

        node.partitionPool.Add(partition)

        for replicaNumber, _ := range replicas {
            if heldPartitionReplicas[partitionNumber] != nil && heldPartitionReplicas[partitionNumber][replicaNumber] {
                continue
            }

            if heldPartitionReplicas[partitionNumber] == nil || !heldPartitionReplicas[partitionNumber][replicaNumber] {
                // This indicates that the node has not yet fully transferred the partition form its old holder
                // start transfer initiates the transfer from the old owner and starts downloading data
                // if another download is not already in progress for that partition
                node.transferAgent.StartTransfer(partitionNumber, replicaNumber)
            }
        }
    }
}

func (node *ClusterNode) startNetworking() error {
    router := node.cloudServer.Router()
    transferAgent := NewDefaultHTTPTransferAgent(node.configController, node.partitionPool)

    node.raftTransport.Attach(router)
    transferAgent.Attach(router)

    node.transferAgent = transferAgent

    go func() {
        node.cloudServer.Start()
    }()

    return nil
}

func (node *ClusterNode) Stop() {
    node.stop()
}

func (node *ClusterNode) stop() {
    node.storageDriver.Close()
    node.configController.Stop()
}

func (node *ClusterNode) ID() uint64 {
    return node.configController.ClusterController().LocalNodeID
}

func (node *ClusterNode) InitializeCluster(settings ClusterSettings) error {
    ctx, cancel := context.WithCancel(context.Background())

    go func() {
        select {
        case <-ctx.Done():
            return
        case <-node.shutdown:
            cancel()
            return
        }
    }()

    Log.Infof("Local node (id = %d) initializing cluster settings (replication_factor = %d, partitions = %d)", node.ID(), settings.ReplicationFactor, settings.Partitions)

    if err := node.configController.ClusterCommand(ctx, ClusterSetReplicationFactorBody{ ReplicationFactor: settings.ReplicationFactor }); err != nil {
        Log.Criticalf("Local node (id = %d) was unable to initialize the replication factor of the new cluster: %v", node.ID(), err.Error())

        return err
    }

    if err := node.configController.ClusterCommand(ctx, ClusterSetPartitionCountBody{ Partitions: settings.Partitions }); err != nil {
        Log.Criticalf("Local node (id = %d) was unable to initialize the partition count factor of the new cluster: %v", node.ID(), err.Error())

        return err
    }

    Log.Infof("Cluster initialization complete!")

    return nil
}

func (node *ClusterNode) JoinCluster(seedHost string, seedPort int) error {
    return nil
}

func (node *ClusterNode) LeaveCluster() error {
    return nil
}

func (node *ClusterNode) ProcessClusterUpdates(deltas []ClusterStateDelta) {
    for _, delta := range deltas {
        switch delta.Type {
        case DeltaNodeAdd:
            Log.Infof("This node (id = %d) was added to a cluster.", node.ID())
            node.joinedCluster <- 1
        case DeltaNodeRemove:
            Log.Infof("This node (id = %d) was removed from its cluster. It will now shut down...", node.ID())
            node.leftCluster <- 1
        case DeltaNodeGainPartitionReplica:
            // Unlock that partition
        case DeltaNodeLosePartitionReplica:
            // Delete that partition
        case DeltaNodeGainPartitionReplicaOwnership:
        case DeltaNodeLosePartitionReplicaOwnership:
        case DeltaSiteAdded:
            // If we are responsible for the partition that this site
            // belongs to then add this site to that partition's site pool
        case DeltaSiteRemoved:
            // If we are responsible for the partition that this site
            // belongs to then remove this site from that partition's site pool
        case DeltaRelayAdded:
            // Nothing to do. The cluster should be aware of this relay
        case DeltaRelayRemoved:
            // disconnect the relay if it's currently connected
        case DeltaRelayMoved:
            // disconnect the relay if it's currently connected
        }
    }
}

/*
func (server *CloudServer) joinCluster() error {
    // send add requests until one is successful
    memberAddress := PeerAddress{
        Host: server.seedHost,
        Port: server.seedPort,
    }
    newMemberConfig := NodeConfig{
        Capacity: server.capacity,
        Address: PeerAddress{
            NodeID: server.clusterController.LocalNodeID,
            Host: server.host,
            Port: server.port,
        },
    }

    for {
        ctx, cancel := context.WithCancel(context.Background())
        wasAdded := false
        stopped := make(chan int)

        // run a goroutine in the background to
        // cancel running add node request when
        // this node is shut down
        go func() {
            defer func() { stopped <- 1 }()

            for {
                select {
                case <-server.joinedCluster:
                    wasAdded = true
                    cancel()
                    return
                case <-ctx.Done():
                    return
                case <-server.stop:
                    cancel()
                    return
                }
            }
        }()

        Log.Infof("Local node (id = %d) is trying to join a cluster through an existing cluster member at %s:%d", server.clusterController.LocalNodeID, server.seedHost, server.seedPort)
        err := server.interClusterClient.AddNode(ctx, memberAddress, newMemberConfig)

        // Cancel to ensure the goroutine gets cleaned up
        cancel()

        // Ensure that the above goroutine has exited and there are no new updates to consume
        <-stopped

        if wasAdded {
            return nil
        }

        if _, ok := err.(DBerror); ok {
            if err.(DBerror) == EDuplicateNodeID {
                Log.Criticalf("Local node (id = %d) request to join the cluster failed because its ID is not unique. This may indicate that the node is trying to use a duplicate ID or it may indicate that a previous proposal that this node made was already accepted and it just hasn't heard about it yet.", server.clusterController.LocalNodeID)
                Log.Criticalf("Local node (id = %d) will now wait one minute to see if it is part of the cluster. If it receives no messages it will shut down", server.clusterController.LocalNodeID)

                select {
                case <-server.joinedCluster:
                    return nil
                case <-server.stop:
                    return EStopped
                case <-time.After(time.Minute):
                    return EDuplicateNodeID
                }
            }
        }

        if err == nil {
            return nil
        }

        Log.Errorf("Local node (id = %d) encountered an error while trying to join cluster: %v", server.clusterController.LocalNodeID, err.Error())
        Log.Infof("Local node (id = %d) will try to join the cluster again in %d seconds", server.clusterController.LocalNodeID, ClusterJoinRetryTimeout)

        select {
        case <-server.joinedCluster:
            // The node has been added to the cluster. The AddNode() request may
            // have been successfully submitted but the response just didn't make
            // it to this node, but it worked. No need to retry joining
            return nil
        case <-server.stop:
            return EStopped
        case <-time.After(time.Second * ClusterJoinRetryTimeout):
            continue
        }
    }


    const (
    ClusterJoinRetryTimeout = 5
)
}*/