package server

import (
    "io"
    "io/ioutil"
    "net"
    "net/http"
    "time"
    "strconv"
    "github.com/gorilla/mux"
    "github.com/gorilla/websocket"
    "net/http/pprof"
    "encoding/json"
    "context"

    . "devicedb/storage"
    . "devicedb/error"
    . "devicedb/logging"
    . "devicedb/cluster"
    . "devicedb/raft"
    . "devicedb/util"
    . "devicedb/client"
)

const (
    raftStorePrefix = iota
)

const (
    ClusterJoinRetryTimeout = 5
)

type CloudServerConfig struct {
    // Path to the storage directory used for database persistent files. This directory keeps track of node data and configuration. It should point to a persistent volume.
    Store string
    // The replication factor the cluster. This parameter is ignored if the node is not being started for the first time or is not the first node in a new cluster.
    ReplicationFactor uint64
    // The number of partitions that the cluster should have. This parameter is ignored if the node is not being started for the first time or is not the first node in a new cluster.
    Partitions uint64
    // The port that the node should listen on and advertise to other nodes in its cluster for communication.
    Port int
    // The host that the node should listen on and advertise to other nodes in its cluster for communication.
    Host string
    // if SeedHost and SeedPort are specified it indicates that this node should join an existing cluster. It means that ReplicationFactor and Partitions will be ignored since this node will adopt the settings dictated by the cluster it is joining.
    // SeedHost is the host name or IP of an existing cluster member used to bootstrap the addition of this node to the cluster
    SeedHost string
    // SeedPort is the port of an existing cluster member used to bootstrap the addition of this node to the cluster
    SeedPort int
    Capacity uint64
}

type CloudServer struct {
    httpServer *http.Server
    listener net.Listener
    storageDriver StorageDriver
    replicationFactor uint64
    partitions uint64
    seedPort int
    seedHost string
    port int
    host string
    capacity uint64
    upgrader websocket.Upgrader
    interClusterClient *Client
    clusterController *ClusterController
    raftNode *RaftNode
    configController *ConfigController
    raftTransportHub *TransportHub
    stop chan int
    joinedCluster chan int
}

func NewCloudServer(serverConfig CloudServerConfig) (*CloudServer, error) {
    upgrader := websocket.Upgrader{
        ReadBufferSize:  1024,
        WriteBufferSize: 1024,
    }
    
    storageDriver := NewLevelDBStorageDriver(serverConfig.Store, nil)
    server := &CloudServer{ 
        storageDriver: storageDriver,
        port: serverConfig.Port,
        host: serverConfig.Host,
        seedHost: serverConfig.SeedHost,
        seedPort: serverConfig.SeedPort,
        capacity: serverConfig.Capacity,
        replicationFactor: serverConfig.ReplicationFactor,
        partitions: serverConfig.Partitions,
        upgrader: upgrader,
        joinedCluster: make(chan int),
    }

    server.interClusterClient = NewClient(ClientConfig{ })

    err := server.storageDriver.Open()
    
    if err != nil {
        if err != ECorrupted {
            Log.Errorf("Error creating server: %v", err.Error())
            
            return nil, err
        }

        Log.Error("Database is corrupted. Attempting automatic recovery now...")

        recoverError := server.recover()

        if recoverError != nil {
            Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())
            Log.Critical("Database daemon will now exit")

            return nil, EStorage
        }

        Log.Info("Database recovery successful!")
    }

    if err := server.initializeConfigController(); err != nil {
        return nil, err
    }

    return server, nil
}

func (server *CloudServer) shouldStartNewCluster() bool {
    return server.seedHost == "" && server.seedPort == 0
}

func (server *CloudServer) initializeConfigController() error {
    raftStore := NewRaftStorage(NewPrefixedStorageDriver([]byte{ raftStorePrefix }, server.storageDriver))

    if err := raftStore.Open(); err != nil {
        Log.Criticalf("Unable to open raft store. Reason: %v", err.Error())

        return EStorage
    }

    nodeID, _ := raftStore.NodeID()

    if nodeID == 0 {
        nodeID = UUID64()

        Log.Infof("Initializing new node with id = %d", nodeID)

        if err := raftStore.SetNodeID(nodeID); err != nil {
            Log.Criticalf("Unable to generate a new node ID. Reason: %v", err.Error())

            return EStorage
        }
    }

    addNodeBody, _ := EncodeClusterCommandBody(ClusterAddNodeBody{
        NodeID: nodeID,
        NodeConfig: NodeConfig{
            Address: PeerAddress{
                NodeID: nodeID,
                Host: server.host,
                Port: server.port,
            },
            Capacity: server.capacity,
        },
    })
    addNodeContext, _ := EncodeClusterCommand(ClusterCommand{ Type: ClusterAddNode, Data: addNodeBody })
    server.raftNode = NewRaftNode(&RaftNodeConfig{
        ID: nodeID,
        CreateClusterIfNotExist: server.shouldStartNewCluster(),
        Context: addNodeContext,
        Storage: raftStore,
        GetSnapshot: func() ([]byte, error) {
            return server.clusterController.State.Snapshot()
        },
    })

    server.clusterController = &ClusterController{
        LocalNodeID: nodeID,
        State: ClusterState{ },
        PartitioningStrategy: &SimplePartitioningStrategy{ },
        LocalUpdates: make(chan ClusterStateDelta),
    }

    server.raftTransportHub = NewTransportHub(nodeID)
    server.raftTransportHub.SetDefaultRoute(server.seedHost, server.seedPort)

    server.configController = NewConfigController(server.raftNode, server.raftTransportHub, server.clusterController)

    return nil
}

func (server *CloudServer) Port() int {
    return server.port
}

func (server *CloudServer) recover() error {
    recoverError := server.storageDriver.Recover()

    if recoverError != nil {
        Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())

        return EStorage
    }

    return nil
}

// starting in different modes...
// If starting up for the first time look at config parameters (-join vs new single node cluster, etc)
// Ignore startup parameters if already started up before
// If this is a new node without a cluster:
//   Is it staged to be the only node in a single node cluster? Nothing to do. 
//      Just start up
//   Is it a new node waiting to join a cluster? 
//      Until it joins a cluster successfully it can't accept any requests. It should just try to join until it is killed
// If this is a node that already is part of a cluster:
//   Just start up
func (server *CloudServer) Start() error {
    r := mux.NewRouter()

    r.HandleFunc("/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
        // Add a node to the cluster
        body, err := ioutil.ReadAll(r.Body)

        if err != nil {
            Log.Warningf("POST /cluster/nodes: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EReadBody.JSON()) + "\n")
            
            return
        }

        var nodeConfig NodeConfig

        if err := json.Unmarshal(body, &nodeConfig); err != nil {
            Log.Warningf("POST /cluster/nodes: Unable to parse node config body")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(ENodeConfigBody.JSON()) + "\n")
            
            return
        }

        if err := server.configController.AddNode(r.Context(), nodeConfig); err != nil {
            Log.Warningf("POST /cluster/nodes: Unable to add node to cluster")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")

    r.HandleFunc("/cluster/nodes/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
        // Remove, replace, or deccommission a node
    }).Methods("DELETE")

    server.raftTransportHub.Attach(r)
    
    r.HandleFunc("/debug/pprof/", pprof.Index)
    r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    r.HandleFunc("/debug/pprof/profile", pprof.Profile)
    r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
    
    server.stop = make(chan int)

    server.httpServer = &http.Server{
        Handler: r,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var err error

    listener, err = net.Listen("tcp", "0.0.0.0:" + strconv.Itoa(server.Port()))
    
    if err != nil {
        Log.Errorf("Error listening on port %d: %v", server.port, err.Error())
        
        server.Stop()
        
        return err
    }
    
    err = server.storageDriver.Open()
    
    if err != nil {
        if err != ECorrupted {
            Log.Errorf("Error opening storage driver: %v", err.Error())
            
            return EStorage
        }

        Log.Error("Database is corrupted. Attempting automatic recovery now...")

        recoverError := server.recover()

        if recoverError != nil {
            Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())
            Log.Critical("Database daemon will now exit")

            return EStorage
        }
    }
    
    server.listener = listener

    // Ensure that node does not get notified of state deltas while log replay
    // is happening
    server.clusterController.DisableNotifications()

    go server.consumeClusterUpdates()

    // Replay config log to restore node config state
    if err := server.configController.Start(); err != nil {
        Log.Errorf("Node %d unable to start: %v", server.clusterController.LocalNodeID, err.Error())

        return err
    }

    server.clusterController.EnableNotifications()

    if !server.clusterController.LocalNodeIsInCluster() && !server.shouldStartNewCluster() {
        // This node isn't yet part of a cluster and is set to join an existing cluster
        Log.Infof("Local node (id = %d) will attempt to join an existing cluster using the seed node at %s:%d", server.clusterController.LocalNodeID, server.seedHost, server.seedPort)
        go server.joinCluster()
    }

    Log.Infof("Node %d listening on port %d", server.clusterController.LocalNodeID, server.port)

    err = server.httpServer.Serve(server.listener)

    Log.Errorf("Node %d server shutting down. Reason: %v", server.clusterController.LocalNodeID, err)

    return err
}

func (server *CloudServer) joinCluster() {
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

        // run a goroutine in the background to
        // cancel running add node request when
        // this node is shut down
        go func() {
            for {
                select {
                case <-server.joinedCluster:
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

        if err == nil {
            // Wait for this node to catch up in the raft config logs
            select {
            case <-server.joinedCluster:
                return
            case <-server.stop:
                return
            }
        }

        Log.Errorf("Local node (id = %d) encountered an error while trying to join cluster: %v", server.clusterController.LocalNodeID, err.Error())
        Log.Infof("Local node (id = %d) will try to join the cluster again in %d seconds", server.clusterController.LocalNodeID, ClusterJoinRetryTimeout)

        select {
        case <-server.joinedCluster:
            // The node has been added to the cluster. The AddNode() request may
            // have been successfully submitted but the response just didn't make
            // it to this node, but it worked. No need to retry joining
            return
        case <-server.stop:
            return
        case <-time.After(time.Second * ClusterJoinRetryTimeout):
            continue
        }
    }
}

func (server *CloudServer) consumeClusterUpdates() {
    for {
        select {
            case delta := <-server.clusterController.LocalUpdates:
                switch delta.Type {
                case DeltaNodeAdd:
                    Log.Infof("This node (id = %d) was added to a cluster.", server.clusterController.LocalNodeID)
                    close(server.joinedCluster)
                case DeltaNodeRemove:
                    Log.Infof("This node (id = %d) was removed from its cluster. It will now shut down...", server.clusterController.LocalNodeID)
                    server.Stop()
                case DeltaNodeGainPartitionReplica:
                case DeltaNodeLosePartitionReplica:
                case DeltaNodeGainToken:
                case DeltaNodeLoseToken:
                }
            case <-server.stop:
                return
        }
    }
}

func (server *CloudServer) Stop() error {
    if server.listener != nil {
        server.listener.Close()
    }
    
    server.configController.Stop()
    server.storageDriver.Close()
    close(server.stop)
    
    return nil
}