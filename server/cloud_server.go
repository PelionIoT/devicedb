package server

import (
    "crypto/tls"
    "net"
    "net/http"
    "time"
    "strconv"
    "github.com/gorilla/mux"
    "sync"

    . "devicedb/logging"
)

/*type RelayHandler struct {
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
}*/

type CloudServerConfig struct {
    NodeID uint64
    ExternalPort int
    ExternalHost string
    InternalPort int
    InternalHost string
    RelayTLSConfig *tls.Config
}

type CloudServer struct {
    httpServer *http.Server
    relayHTTPServer *http.Server
    listener net.Listener
    relayListener net.Listener
    seedPort int
    seedHost string
    externalPort int
    externalHost string
    internalPort int
    internalHost string
    relayTLSConfig *tls.Config
    router *mux.Router
    stop chan int
    nodeID uint64
}

func NewCloudServer(serverConfig CloudServerConfig) *CloudServer {
    server := &CloudServer{
        externalHost: serverConfig.ExternalHost,
        externalPort: serverConfig.ExternalPort,
        internalHost: serverConfig.InternalHost,
        internalPort: serverConfig.InternalPort,
        relayTLSConfig: serverConfig.RelayTLSConfig,
        nodeID: serverConfig.NodeID,
        router: mux.NewRouter(),
    }

    return server
}

func (server *CloudServer) ExternalPort() int {
    return server.externalPort
}

func (server *CloudServer) ExternalHost() string {
    return server.externalHost
}

func (server *CloudServer) InternalPort() int {
    return server.internalPort
}

func (server *CloudServer) InternalHost() string {
    return server.internalHost
}

func (server *CloudServer) Router() *mux.Router {
    return server.router
}

func (server *CloudServer) Start() error {
    server.stop = make(chan int)

    server.httpServer = &http.Server{
        Handler: server.router,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }

    server.relayHTTPServer = &http.Server{
        Handler: server.router,
        WriteTimeout: 15 * time.Second,
        ReadTimeout: 15 * time.Second,
    }
    
    var listener net.Listener
    var relayListener net.Listener
    var err error

    listener, err = net.Listen("tcp", server.InternalHost() + ":" + strconv.Itoa(server.InternalPort()))

    if err != nil {
        Log.Errorf("Error listening on port %d: %v", server.InternalPort(), err.Error())
        
        server.Stop()
        
        return err
    }

    server.listener = listener
    relayListener, err = tls.Listen("tcp", server.ExternalHost() + ":" + strconv.Itoa(server.ExternalPort()), server.relayTLSConfig)

    if err != nil {
        Log.Errorf("Error setting up relay listener on port %d: %v", server.ExternalPort(), err.Error())
        
        server.Stop()
        
        return err
    }
    
    server.relayListener = relayListener

    Log.Infof("Listening external (%s:%d), internal (%s:%d)", server.ExternalHost(), server.ExternalPort(), server.InternalHost(), server.InternalPort())

    var wg sync.WaitGroup
    wg.Add(2)

    go func() {
        err = server.httpServer.Serve(server.listener)
        server.Stop() // to ensure all other listeners shutdown
        wg.Done()
    }()

    go func() {
        err = server.relayHTTPServer.Serve(server.relayListener)
        server.Stop() // to ensure all other listeners shutdown
        wg.Done()
    }()

    wg.Wait()

    Log.Errorf("Server shutting down. Reason: %v", err)

    return err
}

func (server *CloudServer) Stop() {
    if server.listener != nil {
        server.listener.Close()
    }

    if server.relayListener != nil {
        server.relayListener.Close()
    }
}
