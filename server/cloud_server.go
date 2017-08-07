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

    . "devicedb/storage"
    . "devicedb/error"
    . "devicedb/logging"
    . "devicedb/cluster"
    . "devicedb/raft"
    . "devicedb/util"
)

const (
    raftStorePrefix = iota
)

type CloudServerConfig struct {
    Store string
    Port int
    Host string
    SeedPort int
    SeedHost string
    Capacity uint64
}

type CloudServer struct {
    httpServer *http.Server
    listener net.Listener
    storageDriver StorageDriver
    seedPort int
    seedHost string
    port int
    host string
    capacity uint64
    upgrader websocket.Upgrader
    clusterController *ClusterController
    raftNode *RaftNode
    configController *ConfigController
    raftTransportHub *TransportHub
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
        upgrader: upgrader,
    }

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
    }

    server.raftTransportHub = NewTransportHub()

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
    
    server.httpServer = &http.Server{
        Handler: r,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var err error

    listener, err = net.Listen("tcp", "0.0.0.0:" + strconv.Itoa(server.Port()))
    
    if err != nil {
        Log.Errorf("Error listening on port: %d", server.port)
        
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

    Log.Infof("Node %s listening on port %d", "ID", server.port)

    err = server.httpServer.Serve(server.listener)

    Log.Errorf("Node %s server shutting down. Reason: %v", "ID", err)

    return err
}

func (server *CloudServer) Stop() error {
    if server.listener != nil {
        server.listener.Close()
    }
    
    server.storageDriver.Close()
    
    return nil
}