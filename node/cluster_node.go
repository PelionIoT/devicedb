package node

import (
    "context"
    "encoding/binary"
    "fmt"
    "sync"
    "time"

    . "devicedb/client"
    . "devicedb/cluster"
    . "devicedb/error"
    . "devicedb/logging"
    . "devicedb/merkle"
    . "devicedb/partition"
    . "devicedb/raft"
    . "devicedb/routes"
    . "devicedb/server"
    . "devicedb/site"
    . "devicedb/storage"
    . "devicedb/transfer"
    . "devicedb/util"
)

const (
    RaftStoreStoragePrefix = iota
    SiteStoreStoragePrefix = iota
)

const ClusterJoinRetryTimeout = 5

type ClusterNodeConfig struct {
    StorageDriver StorageDriver
    CloudServer *CloudServer
    MerkleDepth uint8
}

type ClusterNode struct {
    interClusterClient *Client
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
    leftClusterResult chan error
    isRunning bool
    shutdown chan int
    empty chan int
    initializedCB func()
    merkleDepth uint8
    shutdownDecommissioner func()
    lock sync.Mutex
}

func New(config ClusterNodeConfig) *ClusterNode {
    if config.MerkleDepth < MerkleMinDepth {
        config.MerkleDepth = MerkleDefaultDepth
    }

    clusterNode := &ClusterNode{
        storageDriver: config.StorageDriver,
        cloudServer: config.CloudServer,
        raftStore: NewRaftStorage(NewPrefixedStorageDriver([]byte{ RaftStoreStoragePrefix }, config.StorageDriver)),
        raftTransport: NewTransportHub(0),
        configControllerBuilder: &ConfigControllerBuilder{ },
        interClusterClient: NewClient(ClientConfig{ }),
        merkleDepth: config.MerkleDepth,
        partitionFactory: NewDefaultPartitionFactory(),
        partitionPool: NewDefaultPartitionPool(),
    }

    return clusterNode
}

func (node *ClusterNode) UseRaftStore(raftStore RaftNodeStorage) {
    node.raftStore = raftStore
}

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
    node.isRunning = true
    node.shutdown = make(chan int)
    node.joinedCluster = make(chan int, 1)
    node.leftCluster = make(chan int, 1)
    node.empty = make(chan int, 1)

    if err := node.openStorageDriver(); err != nil {
        return err
    }

    nodeID, err := node.getNodeID()

    if err != nil {
        return err
    }

    Log.Infof("Local node (id = %d) starting up...", nodeID)

    node.raftTransport.SetLocalPeerID(nodeID)

    clusterHost, clusterPort := options.ClusterAddress()
    node.configControllerBuilder.SetLocalNodeAddress(PeerAddress{ NodeID: nodeID, Host: clusterHost, Port: clusterPort })
    node.configControllerBuilder.SetRaftNodeStorage(node.raftStore)
    node.configControllerBuilder.SetRaftNodeTransport(node.raftTransport)
    node.configControllerBuilder.SetCreateNewCluster(options.ShouldStartCluster())
    node.configController = node.configControllerBuilder.Create()

    stateCoordinator := NewClusterNodeStateCoordinator(&NodeCoordinatorFacade{ node: node }, nil)
    node.configController.OnLocalUpdates(func(deltas []ClusterStateDelta) {
        stateCoordinator.ProcessClusterUpdates(deltas)
    })

    node.configController.Start()
    defer node.Stop()

    if node.configController.ClusterController().LocalNodeWasRemovedFromCluster() {
        Log.Errorf("Local node (id = %d) unable to start because it was removed from the cluster", nodeID)

        return ERemoved
    }

    // It is important to initialize node before networking starts
    // to ensure no cluster config state changes occur while initialize is being called.
    // Initialize needs to set up transfers and partitions with the node's last known
    // state before changes to its partitions ownership and partition transfers
    // occur
    node.transferAgent = NewDefaultHTTPTransferAgent(node.configController, node.partitionPool)
    stateCoordinator.InitializeNodeState()

    serverStopResult := node.startNetworking()
    decommission, err := node.raftStore.IsDecommissioning()

    if err != nil {
        Log.Criticalf("Local node (id = %d) unable to start up since it could not check the decomissioning flag: %v", nodeID, err.Error())

        return err
    }

    if decommission {
        Log.Infof("Local node (id = %d) will resume decommissioning process", nodeID)

        err, result := node.LeaveCluster()

        if err != nil {
            Log.Criticalf("Local node (id = %d) unable to resume decommissioning process: %v", nodeID, err.Error())

            return err
        }

        return <-result
    }
    
    if !node.configController.ClusterController().LocalNodeIsInCluster() || !node.configController.ClusterController().State.ClusterSettings.AreInitialized() {
        if options.ShouldJoinCluster() {
            seedHost, seedPort := options.SeedNode()

            Log.Infof("Local node (id = %d) joining existing cluster. Seed node at %s:%d", nodeID, seedHost, seedPort)

            if err := node.joinCluster(seedHost, seedPort); err != nil {
                Log.Criticalf("Local node (id = %d) unable to join cluster: %v", nodeID, err.Error())

                return err
            }
        } else {
            Log.Infof("Local node (id = %d) creating new cluster...", nodeID)

            if err := node.initializeCluster(options.ClusterSettings); err != nil {
                Log.Criticalf("Local node (id = %d) unable to create new cluster: %v", nodeID, err.Error())

                return err
            }
        }
    }

    node.notifyInitialized()

    select {
    case err := <-serverStopResult:
        Log.Errorf("Local node (id = %d) stopped with error: %v", nodeID, err.Error())
        return err
    case <-node.shutdown:
        return nil
    }
}

func (node *ClusterNode) notifyInitialized() {
    if node.initializedCB != nil {
        node.initializedCB()
    }
}

func (node *ClusterNode) OnInitialized(cb func()) {
    node.initializedCB = cb
}

func (node *ClusterNode) ClusterConfigController() ClusterConfigController {
    return node.configController
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

func (node *ClusterNode) startNetworking() <-chan error {
    router := node.cloudServer.Router()
    clusterEndpoint := &ClusterEndpoint{ ClusterFacade: &ClusterNodeFacade{ node: node } }

    node.raftTransport.Attach(router)
    node.transferAgent.(*HTTPTransferAgent).Attach(router)
    clusterEndpoint.Attach(router)

    startResult := make(chan error)

    go func() {
        startResult <- node.cloudServer.Start()
    }()

    return startResult
}

func (node *ClusterNode) sitePool(partitionNumber uint64) SitePool {
    storageDriver := NewPrefixedStorageDriver(node.sitePoolStorePrefix(partitionNumber), node.storageDriver)
    siteFactory := &CloudSiteFactory{ NodeID: node.Name(), MerkleDepth: node.merkleDepth, StorageDriver: storageDriver }

    return &CloudNodeSitePool{ SiteFactory: siteFactory }
}

func (node *ClusterNode) sitePoolStorePrefix(partitionNumber uint64) []byte {
    prefix := make([]byte, 9)

    prefix[0] = SiteStoreStoragePrefix
    binary.BigEndian.PutUint64(prefix[1:], partitionNumber)

    return prefix
}

func (node *ClusterNode) Stop() {
    node.lock.Lock()
    defer node.lock.Unlock()

    node.stop()
}

func (node *ClusterNode) stop() {
    node.storageDriver.Close()
    node.configController.Stop()
    node.cloudServer.Stop()

    if node.shutdownDecommissioner != nil {
        node.shutdownDecommissioner()
    }

    if node.isRunning {
        node.isRunning = false
        close(node.shutdown)
    }
}

func (node *ClusterNode) ID() uint64 {
    return node.configController.ClusterController().LocalNodeID
}

func (node *ClusterNode) Name() string {
    return "cloud-" + fmt.Sprintf("%d", node.ID())
}

func (node *ClusterNode) initializeCluster(settings ClusterSettings) error {
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

func (node *ClusterNode) joinCluster(seedHost string, seedPort int) error {
    node.raftTransport.SetDefaultRoute(seedHost, seedPort)

    memberAddress := PeerAddress{
        Host: seedHost,
        Port: seedPort,
    }

    newMemberConfig := NodeConfig{
        Capacity: 1,
        Address: PeerAddress{
            NodeID: node.ID(),
            Host: node.cloudServer.InternalHost(),
            Port: node.cloudServer.InternalPort(),
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
                case <-node.joinedCluster:
                    wasAdded = true
                    cancel()
                    return
                case <-ctx.Done():
                    return
                case <-node.shutdown:
                    cancel()
                    return
                }
            }
        }()

        Log.Infof("Local node (id = %d) is trying to join a cluster through an existing cluster member at %s:%d", node.ID(), seedHost, seedPort)
        err := node.interClusterClient.AddNode(ctx, memberAddress, newMemberConfig)

        // Cancel to ensure the goroutine gets cleaned up
        cancel()

        // Ensure that the above goroutine has exited and there are no new updates to consume
        <-stopped

        if wasAdded {
            return nil
        }

        if _, ok := err.(DBerror); ok {
            if err.(DBerror) == EDuplicateNodeID {
                Log.Criticalf("Local node (id = %d) request to join the cluster failed because its ID is not unique. This may indicate that the node is trying to use a duplicate ID or it may indicate that a previous proposal that this node made was already accepted and it just hasn't heard about it yet.", node.ID())
                Log.Criticalf("Local node (id = %d) will now wait one minute to see if it is part of the cluster. If it receives no messages it will shut down", node.ID())

                select {
                case <-node.joinedCluster:
                    return nil
                case <-node.shutdown:
                    return EStopped
                case <-time.After(time.Minute):
                    return EDuplicateNodeID
                }
            }
        }

        if err != nil {
            Log.Errorf("Local node (id = %d) encountered an error while trying to join cluster: %v", node.ID(), err.Error())
            Log.Infof("Local node (id = %d) will try to join the cluster again in %d seconds", node.ID(), ClusterJoinRetryTimeout)

            select {
            case <-node.joinedCluster:
                // The node has been added to the cluster. The AddNode() request may
                // have been successfully submitted but the response just didn't make
                // it to this node, but it worked. No need to retry joining
                return nil
            case <-node.shutdown:
                return EStopped
            case <-time.After(time.Second * ClusterJoinRetryTimeout):
                continue
            }
        }

        select {
        case <-node.joinedCluster:
            return nil
        case <-node.shutdown:
            return EStopped
        }
    }
}

func (node *ClusterNode) LeaveCluster() (error, <-chan error) {
    node.lock.Lock()
    defer node.lock.Unlock()

    // allow at mot one decommissioner
    if node.shutdownDecommissioner != nil {
        return nil, node.leftClusterResult
    }

    Log.Infof("Local node (id = %d) is being put into decommissioning mode", node.ID())

    if err := node.raftStore.SetDecommissioningFlag(); err != nil {
        Log.Errorf("Local node (id = %d) was unable to be put into decommissioning mode: %v", node.ID(), err.Error())

        return err, nil
    }

    ctx, cancel := context.WithCancel(context.Background())
    node.shutdownDecommissioner = cancel
    node.leftClusterResult = make(chan error, 1)

    go func() {
        node.leftClusterResult <- node.decommission(ctx)
    }()

    return nil, node.leftClusterResult
}

func (node *ClusterNode) IsLeavingCluster() bool {
    node.lock.Lock()
    defer node.lock.Unlock()

    return node.shutdownDecommissioner != nil
}

func (node *ClusterNode) decommission(ctx context.Context) error {
    Log.Infof("Local node (id = %d) starting decommissioning process...", node.ID())

    localNodeConfig := node.configController.ClusterController().LocalNodeConfig()

    if localNodeConfig == nil {
        Log.Criticalf("Local node (id = %d) unable to continue decommissioning process since its node config is not in the cluster config", node.ID())

        return ERemoved
    }

    if localNodeConfig.Capacity != 0 {
        Log.Infof("Local node (id = %d) decommissioning (1/4): Giving up tokens...", node.ID())

        if err := node.configController.ClusterCommand(ctx, ClusterUpdateNodeBody{ NodeID: node.ID(), NodeConfig: NodeConfig{ Capacity: 0, Address: localNodeConfig.Address } }); err != nil {
            Log.Criticalf("Local node (id = %d) was unable to give up its tokens: %v", node.ID(), err.Error())

            return err
        }
    }

    // Transfers should be stopped anyway once the capacity is set to zero and this node no longer owns
    // any tokens but call it here to make sure all have stopped by this point.
    node.transferAgent.StopAllTransfers()
    heldPartitionReplicas := node.configController.ClusterController().LocalNodeHeldPartitionReplicas()

    if len(heldPartitionReplicas) > 0 {
        Log.Infof("Local node (id = %d) decommissioning (2/4): Locking partitions...", node.ID())

        // Write lock partitions that are still held. This should occur anyway since
        // The node no longer owns these partitions but calling it here ensures this
        // invariant holds for the next steps of the decommissioning process
        for _, partitionReplica := range heldPartitionReplicas {
            partition := node.partitionPool.Get(partitionReplica.Partition)

            if partition != nil {
                Log.Debugf("Local node (id = %d) decommissioning (2/4): Write locking partition %d", node.ID(), partition.Partition())

                node.transferAgent.EnableOutgoingTransfers(partition.Partition())
                partition.LockWrites()
            }
        }

        Log.Infof("Local node (id = %d) decommissioning (3/4): Transferring partition data...", node.ID())

        // Wait for all partition data to be transferred away from this node. This ensures that
        // the data that this node held is replicated elsewhere before it removes itself from the
        // cluster permanently.
        select {
        case <-node.leftCluster:
            return ERemoved
        case <-node.empty:
        case <-ctx.Done():
            return ECancelled
        }
    }

    Log.Infof("Local node (id = %d) decommissioning (4/4): Leaving cluster...", node.ID())

    if err := node.configController.RemoveNode(ctx, node.ID()); err != nil {
        Log.Criticalf("Local node (id = %d) was unable to leave cluster: %v", node.ID(), err.Error())

        return err
    }

    return EDecommissioned
}

type ClusterNodeFacade struct {
    node *ClusterNode
}

func (clusterFacade *ClusterNodeFacade) AddNode(ctx context.Context, nodeConfig NodeConfig) error {
    return clusterFacade.node.configController.AddNode(ctx, nodeConfig)
}

func (clusterFacade *ClusterNodeFacade) RemoveNode(ctx context.Context, nodeID uint64) error {
    return clusterFacade.node.configController.RemoveNode(ctx, nodeID)
}

func (clusterFacade *ClusterNodeFacade) ReplaceNode(ctx context.Context, nodeID uint64, replacementNodeID uint64) error {
    return clusterFacade.node.configController.ReplaceNode(ctx, nodeID, replacementNodeID)
}

func (clusterFacade *ClusterNodeFacade) ClusterClient() *Client {
    return clusterFacade.node.interClusterClient
}

func (clusterFacade *ClusterNodeFacade) Decommission() error {
    err, _ := clusterFacade.node.LeaveCluster()

    return err
}

func (clusterFacade *ClusterNodeFacade) LocalNodeID() uint64 {
    return clusterFacade.node.ID()
}

func (clusterFacade *ClusterNodeFacade) PeerAddress(nodeID uint64) PeerAddress {
    return clusterFacade.node.configController.ClusterController().ClusterMemberAddress(nodeID)
}
