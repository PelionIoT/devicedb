// Node Lifecycle
// (Startup) -> [Joined] -> [Decommissioning] -> [Removed] -> (Shutdown)
// Note: startup and shutdown do not necessarily correlate with actual process restarts. Startup corresponds with the time that a new node
// is not yet joined to a cluster. Shutdown indicates the time after the node has been removed from the cluster. If a node is restarted after
// it has been removed from the cluster it will not allow the startup to occur.
//
// A node in the joined state represents a node that has been added to a cluster
// A node in the decommissioning state represents a node that has been told to gracefully remove itself from the cluster after handing off its data
// A node in the removed state represents a node that has been removed from the cluster and can no longer accept any requests on behalf of the cluster
// A node can skip from joined to removed if forcefully removed
//
// Joined -          Cluster config says I am part of the cluster and decommissioning flag has not been set
// Decommissioning - Cluster config says I am part of the cluster and decommissioning flag has been set
// Removed -         Cluster config says I am not part of the cluster
//
// Decommissioning States
// [Decommissioning] ---proposes capacity change--> [Partition Ownership Handed Off] ---holds no more replicas---> [Data Ownership Handed Off] ----proposes removal---> [Removed from cluster]
// Decommissioning State is not tied to raft log state. It is just a flag on a node that indicates its mode of operation
// Relay client (relay talking to cluster)
// Cloud client (cloud app talking to cluster)
// ...
// Relay Cert CA
// Client Cert CA
// Server Cert + Server Key

package server

import (
    "io"
    "io/ioutil"
    "crypto/tls"
    "net"
    "net/http"
    "time"
    "strconv"
    "github.com/gorilla/mux"
    "github.com/gorilla/websocket"
    "net/http/pprof"
    "encoding/json"
    "context"
    "sync"
    "errors"

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

type RelayHandler struct {
    upgrader websocket.Upgrader
}

func NewRelayHandler() *RelayHandler {
    return &RelayHandler{
        upgrader: websocket.Upgrader{
            ReadBufferSize:  1024,
            WriteBufferSize: 1024,
        },
    }
}

func (rh *RelayHandler) extractRelayID(conn *tls.Conn) (string, error) {
    verifiedChains := conn.ConnectionState().VerifiedChains
    
    if len(verifiedChains) != 1 {
        return "", errors.New("Invalid client certificate")
    }
    
    relayID:= verifiedChains[0][0].Subject.CommonName
    
    return relayID, nil
}

func (rh *RelayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    connection, err := rh.upgrader.Upgrade(w, r, nil)
    
    if err != nil {
        return
    }
 
    conn := connection.UnderlyingConn()
    relayID, err := rh.extractRelayID(conn.(*tls.Conn))

    if err != nil {
        conn.Close()

        return
    }

    // rh.relayHub.HandleRelayConnection(relayID, connection)
    Log.Infof("Relay %s connected", relayID)
}

type SwappableHandler struct {
    normalRouter *mux.Router
    decommissioningRouter *mux.Router
    router *mux.Router
    swapLock sync.RWMutex
}

func NewSwappableHandler(normalRouter, decommissioningRouter *mux.Router) *SwappableHandler {
    return &SwappableHandler{
        normalRouter: normalRouter,
        decommissioningRouter: decommissioningRouter,
        router: normalRouter,
    }
}

func (sh *SwappableHandler) SwitchToDecommissioningMode() {
    sh.swapLock.Lock()
    sh.router = sh.decommissioningRouter
    sh.swapLock.Unlock()
}

func (sh *SwappableHandler) isInDecommissioningMode() bool {
    return sh.router == sh.decommissioningRouter
}

func (sh *SwappableHandler) normalRouterHasMatch(req *http.Request) bool {
    return sh.routerHasMatch(sh.normalRouter, req)
}

func (sh *SwappableHandler) decommissioningRouterHasMatch(req *http.Request) bool {
    return sh.routerHasMatch(sh.decommissioningRouter, req)
}

func (sh *SwappableHandler) routerHasMatch(router *mux.Router, req *http.Request) bool {
    var match mux.RouteMatch

    return router.Match(req, &match)
}

func (sh *SwappableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    sh.swapLock.RLock()
    defer sh.swapLock.RUnlock()

    if sh.isInDecommissioningMode() {
        if sh.normalRouterHasMatch(r) && !sh.decommissioningRouterHasMatch(r) {
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusConflict)
            io.WriteString(w, "\n")

            return
        }
    }

    sh.router.ServeHTTP(w, r)
}

type CloudServerConfig struct {
    // If specified this is the cluster node ID this node will use. It is overridden if the node was already assigned a node ID
    NodeID uint64
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
    // The port that the node should listen on for TLS based relay connections
    RelayPort int
    // The host that the node should listen on for TLS based relay connections
    RelayHost string
    // The relay TLS config
    RelayTLSConfig *tls.Config
    // if SeedHost and SeedPort are specified it indicates that this node should join an existing cluster. It means that ReplicationFactor and Partitions will be ignored since this node will adopt the settings dictated by the cluster it is joining.
    // SeedHost is the host name or IP of an existing cluster member used to bootstrap the addition of this node to the cluster
    SeedHost string
    // SeedPort is the port of an existing cluster member used to bootstrap the addition of this node to the cluster
    SeedPort int
    Capacity uint64
}

type CloudServer struct {
    httpServer *http.Server
    relayHTTPServer *http.Server
    listener net.Listener
    relayListener net.Listener
    storageDriver StorageDriver
    replicationFactor uint64
    partitions uint64
    seedPort int
    seedHost string
    port int
    host string
    relayPort int
    relayHost string
    relayTLSConfig *tls.Config
    capacity uint64
    interClusterClient *Client
    clusterController *ClusterController
    raftNode *RaftNode
    raftStore *RaftStorage
    configController *ConfigController
    raftTransportHub *TransportHub
    handler *SwappableHandler
    relayHandler *RelayHandler
    stop chan int
    joinedCluster chan int
    leftCluster chan int
    decommission chan int
    join chan int
    onJoinClusterCB func()
    onLeaveClusterCB func()
    nodeID uint64
}

func NewCloudServer(serverConfig CloudServerConfig) (*CloudServer, error) {
    storageDriver := NewLevelDBStorageDriver(serverConfig.Store, nil)
    server := &CloudServer{ 
        storageDriver: storageDriver,
        port: serverConfig.Port,
        host: serverConfig.Host,
        relayPort: serverConfig.RelayPort,
        relayHost: serverConfig.RelayHost,
        relayTLSConfig: serverConfig.RelayTLSConfig,
        seedHost: serverConfig.SeedHost,
        seedPort: serverConfig.SeedPort,
        capacity: serverConfig.Capacity,
        replicationFactor: serverConfig.ReplicationFactor,
        partitions: serverConfig.Partitions,
        decommission: make(chan int, 1),
        join: make(chan int, 1),
        joinedCluster: make(chan int, 1),
        leftCluster: make(chan int, 1),
        nodeID: serverConfig.NodeID,
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

    server.initializeHandler()

    return server, nil
}

func (server *CloudServer) shouldStartNewCluster() bool {
    return server.seedHost == "" && server.seedPort == 0
}

func (server *CloudServer) initializeConfigController() error {
    raftStore := NewRaftStorage(NewPrefixedStorageDriver([]byte{ raftStorePrefix }, server.storageDriver))

    server.raftStore = raftStore

    if err := raftStore.Open(); err != nil {
        Log.Criticalf("Unable to open raft store. Reason: %v", err.Error())

        return EStorage
    }

    nodeID, _ := raftStore.NodeID()

    if nodeID == 0 {
        if server.nodeID != 0 {
            nodeID = server.nodeID
        } else {
            nodeID = UUID64()
            server.nodeID = nodeID
        }

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
        LocalUpdates: make(chan []ClusterStateDelta),
    }

    server.raftTransportHub = NewTransportHub(nodeID)
    server.raftTransportHub.SetDefaultRoute(server.seedHost, server.seedPort)

    server.configController = NewConfigController(server.raftNode, server.raftTransportHub, server.clusterController)

    return nil
}

func (server *CloudServer) initializeHandler() {
    normalRouter := mux.NewRouter()
    decommissioningRouter := mux.NewRouter()

    normalRouter.HandleFunc("/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
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
            Log.Warningf("POST /cluster/nodes: Unable to add node to cluster: %v", err.Error())

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)

            if err == ECancelConfChange {
                io.WriteString(w, string(EDuplicateNodeID.JSON()) + "\n")
            } else {
                io.WriteString(w, string(EProposalError.JSON()) + "\n")
            }
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")

    normalRouter.HandleFunc("/cluster/nodes/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
        // Remove, replace, or deccommission a node
        query := r.URL.Query()
        _, wasForwarded := query["forwarded"]
        _, replace := query["replace"]
        _, decommission := query["decommission"]

        if replace && decommission {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Both the replace and decommission query parameters are set. This is not allowed")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        nodeID, err := strconv.ParseUint(mux.Vars(r)["nodeID"], 10, 64)

        if err != nil {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Invalid node ID")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        if nodeID == 0 {
            nodeID = server.clusterController.LocalNodeID
        }

        if decommission {
            if nodeID == server.clusterController.LocalNodeID {
                if err := server.raftStore.SetDecommissioningFlag(); err != nil {
                    Log.Warningf("DELETE /cluster/nodes/{nodeID}: Encountered an error while setting the decommissioning flag: %v", err.Error())
                    
                    w.Header().Set("Content-Type", "application/json; charset=utf8")
                    w.WriteHeader(http.StatusInternalServerError)
                    io.WriteString(w, "\n")
                    
                    return
                }

                go server.leaveCluster()

                return
            }

            if wasForwarded {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Received a forwarded decommission request but we're not the correct node")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusForbidden)
                io.WriteString(w, "\n")
                
                return
            } 
            
            // forward the request to another node
            peerAddress := server.raftTransportHub.PeerAddress(nodeID)

            if peerAddress == nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Unable to forward decommission request since this node doesn't know how to contact the decommissioned node")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadGateway)
                io.WriteString(w, "\n")
                
                return
            }

            err := server.interClusterClient.RemoveNode(r.Context(), *peerAddress, nodeID, 0, true, true)

            if err != nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Error forwarding decommission request: %v", err.Error())
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadGateway)
                io.WriteString(w, "\n")
                
                return
            }

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusOK)
            io.WriteString(w, "\n")

            return
        }

        var replacementNodeID uint64

        if replace {
            replacementNodeID, err = strconv.ParseUint(query["replace"][0], 10, 64)

            if err != nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Invalid replacement node ID")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadRequest)
                io.WriteString(w, "\n")
                
                return
            }
        }

        if replacementNodeID != 0 {
            err = server.configController.ReplaceNode(r.Context(), nodeID, replacementNodeID)
        } else {
            err = server.configController.RemoveNode(r.Context(), nodeID)
        }

        if err != nil {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Unable to remove node from the cluster: %v", err.Error())
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("DELETE")

    server.raftTransportHub.Attach(normalRouter)
    server.raftTransportHub.Attach(decommissioningRouter)

    normalRouter.HandleFunc("/debug/pprof/", pprof.Index)
    normalRouter.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    normalRouter.HandleFunc("/debug/pprof/profile", pprof.Profile)
    normalRouter.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
    decommissioningRouter.HandleFunc("/debug/pprof/", pprof.Index)
    decommissioningRouter.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    decommissioningRouter.HandleFunc("/debug/pprof/profile", pprof.Profile)
    decommissioningRouter.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

    server.relayHandler = NewRelayHandler()
    server.handler = NewSwappableHandler(normalRouter, decommissioningRouter)
}

func (server *CloudServer) Port() int {
    return server.port
}

func (server *CloudServer) Host() string {
    return server.host
}

func (server *CloudServer) RelayPort() int {
    return server.relayPort
}

func (server *CloudServer) RelayHost() string {
    return server.relayHost
}

func (server *CloudServer) ClusterController() *ClusterController {
    return server.clusterController
}

func (server *CloudServer) OnJoinCluster(cb func()) {
    server.onJoinClusterCB = cb
}

func (server *CloudServer) notifyJoinCluster() {
    if server.onJoinClusterCB != nil {
        server.onJoinClusterCB()
    }
}

func (server *CloudServer) OnLeaveCluster(cb func()) {
    server.onLeaveClusterCB = cb
}

func (server *CloudServer) recover() error {
    recoverError := server.storageDriver.Recover()

    if recoverError != nil {
        Log.Criticalf("Unable to recover corrupted database. Reason: %v", recoverError.Error())

        return EStorage
    }

    return nil
}

func (server *CloudServer) Start() error {
    server.stop = make(chan int)

    server.httpServer = &http.Server{
        Handler: server.handler,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }

    server.relayHTTPServer = &http.Server{
        Handler: server.relayHandler,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var relayListener net.Listener
    var err error

    listener, err = net.Listen("tcp", server.Host() + ":" + strconv.Itoa(server.Port()))

    if err != nil {
        Log.Errorf("Error listening on port %d: %v", server.port, err.Error())
        
        server.shutdown()
        
        return err
    }

    server.listener = listener
    relayListener, err = tls.Listen("tcp", server.RelayHost() + ":" + strconv.Itoa(server.RelayPort()), server.relayTLSConfig)

    if err != nil {
        Log.Errorf("Error setting up relay listener on port %d: %v", server.RelayPort(), err.Error())
        
        server.shutdown()
        
        return err
    }
    
    server.relayListener = relayListener
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

    server.configController.OnLocalUpdates(func(deltas []ClusterStateDelta) {
        server.processLocalUpdates(deltas)
    })

    // Replay config log to restore node config state
    if err := server.configController.Start(); err != nil {
        Log.Errorf("Node %d unable to start: %v", server.clusterController.LocalNodeID, err.Error())

        return err
    }

    decommission, err := server.raftStore.IsDecommissioning()

    if err != nil {
        Log.Errorf("Node %d unable to start because it was unable to check its decommissioning state: %v", server.clusterController.LocalNodeID, err.Error())

        server.shutdown()

        return err
    }

    if server.clusterController.LocalNodeWasRemovedFromCluster() {
        Log.Errorf("Node %d was removed from a cluster. It cannot be restarted.", server.clusterController.LocalNodeID)

        server.shutdown()

        return nil
    }

    if decommission {
        Log.Infof("Local node (id = %d) will resume decommissioning process", server.clusterController.LocalNodeID)
        server.handler.SwitchToDecommissioningMode()
        server.decommission <- 1
    } else if !server.clusterController.LocalNodeIsInCluster() && !server.shouldStartNewCluster() {
        Log.Infof("Local node (id = %d) will attempt to join an existing cluster using the seed node at %s:%d", server.clusterController.LocalNodeID, server.seedHost, server.seedPort)
        server.join <- 1
    }

    if server.shouldStartNewCluster() && !server.clusterController.ClusterIsInitialized() {
        if err := server.initCluster(); err != nil {
            server.shutdown()

            return err
        }
    }

    go func() {
        server.run()
        server.shutdown()
    }()

    Log.Infof("Node %d listening external (%s:%d), internal (%s:%d)", server.clusterController.LocalNodeID, server.RelayHost(), server.RelayPort(), server.Host(), server.Port())

    var wg sync.WaitGroup
    wg.Add(2)

    go func() {
        err = server.httpServer.Serve(server.listener)
        server.shutdown() // to ensure all other listeners shutdown
        wg.Done()
    }()

    go func() {
        err = server.relayHTTPServer.Serve(server.relayListener)
        server.shutdown() // to ensure all other listeners shutdown
        wg.Done()
    }()

    wg.Wait()

    Log.Errorf("Node %d server shutting down. Reason: %v", server.clusterController.LocalNodeID, err)

    return err
}

func (server *CloudServer) processLocalUpdates(deltas []ClusterStateDelta) {
    for _, delta := range deltas {
        switch delta.Type {
        case DeltaNodeAdd:
            Log.Infof("This node (id = %d) was added to a cluster.", server.clusterController.LocalNodeID)

            server.joinedCluster <- 1
            server.notifyJoinCluster()
        case DeltaNodeRemove:
            Log.Infof("This node (id = %d) was removed from its cluster. It will now shut down...", server.clusterController.LocalNodeID)
            server.leftCluster <- 1
        case DeltaNodeGainPartitionReplica:
            // Unlock that partition
        case DeltaNodeLosePartitionReplica:
            // Delete that partition
        case DeltaNodeGainPartitionReplicaOwnership:
            // Create that partition
        case DeltaNodeLosePartitionReplicaOwnership:
            // Lock that partition
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

func (server *CloudServer) run() {
    for {
        select {
            case <-server.join:
                err := server.joinCluster()

                if _, ok := err.(DBerror); ok {
                    if err.(DBerror) == EDuplicateNodeID {
                        return
                    }
                }

                if err == EStopped {
                    return
                }
            case <-server.decommission:
                if err := server.raftStore.SetDecommissioningFlag(); err != nil {
                    Log.Errorf("Unable to start decommissioning: Encountered an error while setting the decommissioning flag: %v", err.Error())

                    break
                }

                server.handler.SwitchToDecommissioningMode()
                server.leaveCluster()
                return
            case <-server.leftCluster:
                return
            case <-server.stop:
                return
        }
    }
}

func (server *CloudServer) initCluster() error {
    ctx, cancel := context.WithCancel(context.Background())

    go func() {
        select {
        case <-ctx.Done():
            return
        case <-server.stop:
            cancel()
            return
        }
    }()

    Log.Infof("Initializing cluster settings (replication_factor = %d, partitions = %d)", server.replicationFactor, server.partitions)

    if err := server.configController.ClusterCommand(ctx, ClusterSetReplicationFactorBody{ ReplicationFactor: server.replicationFactor }); err != nil {
        Log.Criticalf("Local node (id = %d) was unable to initialize the replication factor of the new cluster: %v", server.clusterController.LocalNodeID, err.Error())

        return err
    }

    if err := server.configController.ClusterCommand(ctx, ClusterSetPartitionCountBody{ Partitions: server.partitions }); err != nil {
        Log.Criticalf("Local node (id = %d) was unable to initialize the partition count factor of the new cluster: %v", server.clusterController.LocalNodeID, err.Error())

        return err
    }

    Log.Infof("Cluster initialization complete!")

    return nil
}

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
}

func (server *CloudServer) leaveCluster() {
    // Step 1: Propose change capacity to zero
    if server.giveUpTokens() {
        return
    }

    Log.Infof("Local node (id = %d) is decommissioning: Successfully relinquished its partitions and will now wait for its data to be transferred to other nodes.", server.clusterController.LocalNodeID)

    // Step 2: Wait for data to be transferred to other nodes
    if server.handOffData() {
        return
    }

    Log.Infof("Local node (id = %d) is decommissioning: Successfully transferred all data to other nodes and will now remove itself from the cluster.", server.clusterController.LocalNodeID)

    // Step 3: Propose removal of this node from the cluster
    server.removeSelfFromCluster()
}

func (server *CloudServer) giveUpTokens() (cancelDecommissioning bool) {
    localNodeConfig := server.clusterController.LocalNodeConfig()

    newNodeConfig := *localNodeConfig
    newNodeConfig.Capacity = 0

    for {
        ctx, cancel := context.WithCancel(context.Background())
        wasRemovedFromCluster := false

        // run a goroutine in the background to
        // cancel running add node request when
        // this node is shut down
        go func() {
            for {
                select {
                case <-server.leftCluster:
                    Log.Infof("This node (id = %d) was removed from the cluster before it could be fully decommissioned. It will shut down now.", server.clusterController.LocalNodeID)
                    cancel()
                    wasRemovedFromCluster = true
                    return
                case <-ctx.Done():
                    return
                case <-server.stop:
                    cancel()
                    return
                }
            }
        }()

        Log.Infof("Local node (id = %d) is decommissioning: Trying to give up its partitions to other cluster members", server.clusterController.LocalNodeID)
        err := server.configController.ClusterCommand(ctx, ClusterUpdateNodeBody{ NodeID: server.clusterController.LocalNodeID, NodeConfig: newNodeConfig })

        // Cancel to ensure the goroutine gets cleaned up
        cancel()

        if wasRemovedFromCluster {
            cancelDecommissioning = true

            return
        }

        if err == nil {
            cancelDecommissioning = false

            return
        }

        Log.Errorf("Local node (id = %d) encountered an error while trying to decommission itself: %v", server.clusterController.LocalNodeID, err.Error())
        Log.Infof("Local node (id = %d) will try to decommission itself again in %d seconds", server.clusterController.LocalNodeID, ClusterJoinRetryTimeout)

        select {
        case <-server.leftCluster:
            Log.Infof("This node (id = %d) was removed from the cluster before it could be fully decommissioned. It will shut down now.", server.clusterController.LocalNodeID)
            cancelDecommissioning = true
            return
        case <-server.stop:
            cancelDecommissioning = true
            return
        case <-time.After(time.Second * ClusterJoinRetryTimeout):
            continue
        }
    }
}

func (server *CloudServer) handOffData() (cancelDecommissioning bool) {
    originalReplicaCount := server.clusterController.LocalPartitionReplicasCount()

    // Step 2: Wait until we lose all partition replicas
    for server.clusterController.LocalPartitionReplicasCount() != 0 {
        select {
        case deltas := <-server.clusterController.LocalUpdates:
            for _, delta := range deltas {
                if delta.Type == DeltaNodeLosePartitionReplica {
                    remainingReplicas := server.clusterController.LocalPartitionReplicasCount()
                    deltaBody := delta.Delta.(NodeLosePartitionReplica)

                    Log.Infof("Local node (id = %d) successfully transferred partition replica %d-%d. Transferred (%d/%d)", server.clusterController.LocalNodeID, deltaBody.Partition, deltaBody.Replica, remainingReplicas, originalReplicaCount)
                }
            }
        case <-server.stop:
            cancelDecommissioning = true

            return
        }
    }

    return false
}

func (server *CloudServer) removeSelfFromCluster() {
    for {
        ctx, cancel := context.WithCancel(context.Background())
        wasRemovedFromCluster := false

        // run a goroutine in the background to
        // cancel running add node request when
        // this node is shut down
        go func() {
            for {
                select {
                case <-server.leftCluster:
                    Log.Infof("This node (id = %d) was removed from the cluster. It will shut down now.", server.clusterController.LocalNodeID)
                    cancel()
                    wasRemovedFromCluster = true
                    return
                case <-ctx.Done():
                    return
                case <-server.stop:
                    cancel()
                    return
                }
            }
        }()

        Log.Infof("Local node (id = %d) is trying to remove itself from the cluster.", server.clusterController.LocalNodeID)
        err := server.configController.RemoveNode(ctx, server.clusterController.LocalNodeID)

        // Cancel to ensure the goroutine gets cleaned up
        cancel()

        if wasRemovedFromCluster {
            return
        }

        if err == nil {
            break
        }

        Log.Errorf("Local node (id = %d) encountered an error while trying to remove itself from the cluster: %v", server.clusterController.LocalNodeID, err.Error())
        Log.Infof("Local node (id = %d) will try to remove itself from the cluster again in %d seconds", server.clusterController.LocalNodeID, ClusterJoinRetryTimeout)

        select {
        case <-server.leftCluster:
            Log.Infof("This node (id = %d) was removed from the cluster. It will shut down now.", server.clusterController.LocalNodeID)
            return
        case <-server.stop:
            return
        case <-time.After(time.Second * ClusterJoinRetryTimeout):
            continue
        }
    }
}

func (server *CloudServer) shutdown() {
    if server.listener != nil {
        server.listener.Close()
    }

    if server.relayListener != nil {
        server.relayListener.Close()
    }
    
    server.configController.Stop()
    server.storageDriver.Close()
}

func (server *CloudServer) Stop() error {
    server.shutdown()
    close(server.stop)
    
    return nil
}