package main

import (
    "flag"
    "fmt"
    "os"
    "devicedb/cluster"
    "strings"
    "strconv"
    "errors"
    "time"
    "sync"
    "context"
    "io/ioutil"
    "crypto/tls"
    "crypto/x509"

    . "devicedb/client"
    . "devicedb/server"
    "devicedb/storage"
    "devicedb/node"
    . "devicedb/version"
    . "devicedb/compatibility"
    . "devicedb/shared"
    . "devicedb/logging"
    . "devicedb/util"
    . "devicedb/raft"
    . "devicedb/merkle"
    . "devicedb/bucket"
    . "devicedb/data"
)

var templateConfig string =
`# The db field specifies the directory where the database files reside on
# disk. If it doesn't exist it will be created.
# **REQUIRED**
db: /tmp/devicedb

# The port field specifies the port number on which to run the database server
port: 9090

# The sync session limit is the number of sync sessions that can happen
# concurrently. Adjusting this field allows the database node to synchronize
# with its peers more or less quickly. It is reccomended that this field be
# half the number of cores on the machine where the devicedb node is running
# **REQUIRED**
syncSessionLimit: 2

# The sync session period is the time in milliseconds that determines the rate
# at which sync sessions are initiated with neighboring peers. Adjusting this
# too high will result in unwanted amounts of network traffic and too low may
# result in slow convergence rates for replicas that have not been in contact
# for some time. A rate on the order of seconds is reccomended generally
# **REQUIRED**
syncSessionPeriod: 1000

# This field adjusts the maximum number of objects that can be transferred in
# one sync session. A higher number will result in faster convergence between
# database replicas. This field must be positive and defaults to 1000
syncExplorationPathLimit: 1000

# In addition to background syncing, updates can also be forwarded directly
# to neighbors when a connection is established in order to reduce the time
# that replicas remain divergent. An update will immediately get forwarded
# to the number of connected peers indicated by this number. If this value is
# zero then the update is forwarded to ALL connected peers. In a small network
# of nodes it may be better to set this to zero.
# **REQUIRED**
syncPushBroadcastLimit: 0

# Garbage collection settings determine how often and to what degree tombstones,
# that is markers of deletion, are removed permananetly from the database 
# replica. The default values are the minimum allowed settings for these
# properties.

# The garbage collection interval is the amount of time between garbage collection
# sweeps in milliseconds. The lowest it can be set is every ten mintues as shown
# below but could very well be set for once a day, or once a week without much
# ill effect depending on the use case or how aggresively disk space needs to be
# preserved
gcInterval: 300000

# The purge age defines the age past which tombstone will be purged from storage
# Tombstones are markers of key deletion and need to be around long enough to
# propogate through the network of database replicas and ensure a deletion happens
# Setting this value too low may cause deletions that don't propogate to all replicas
# if nodes are often disconnected for a long time. Setting it too high may mean
# that more disk space is used than is needed keeping around old tombstones for 
# keys that will no longer be used. This field is also in milliseconds
gcPurgeAge: 600000

# This field can be used to specify how this node handles time-series data.
# These settings adjust how and when historical data is purged from the
# history. If this field is not specified then default values are used.
# history:
#    # When this flag is true items in the history are purged from the log
#    # after they are successfully forwarded to the cloud. When set to false
#    # items are only purged after. It defaults to false if not specified
#    purgeOnForward: false
#    # This setting controls the amount of events that are left in the log
#    # before purging the oldest logged events. It is set to 0 by default
#    # which means old events will never be purged from the log
#    eventLimit: 1000

# The merkle depth adjusts how efficiently the sync process resolves
# differences between database nodes. A rule of thumb is to set this as high
# as memory constraints allow. Estimated memory overhead for a given depth is
# calculated with the formula: M = 3*(2^(d + 4)). The following table gives a
# quick reference to choose an appropriate depth.
#
# depth   |   memory overhead
# 2       |   192         bytes  (0.1      KiB)
# 4       |   768         bytes  (0.8      KiB)
# 6       |   3072        bytes  (3.0      KiB)
# 8       |   12288       bytes  (12       KiB)
# 10      |   49152       bytes  (48       KiB)
# 12      |   196608      bytes  (192      KiB) (0.2   MiB)
# 14      |   786432      bytes  (768      KiB) (0.7   MiB)
# 16      |   3145728     bytes  (3072     KiB) (3.0   MiB)
# 18      |   12582912    bytes  (12288    KiB) (12    MiB)
# 20      |   50331648    bytes  (49152    KiB) (48    MiB)
# 22      |   201326592   bytes  (196608   KiB) (192   MiB) (0.2 GiB)
# 24      |   805306368   bytes  (786432   KiB) (768   MiB) (0.8 GiB)
# 26      |   3221225472  bytes  (3145728  KiB) (3072  MiB) (3   GiB)
# 28      |   12884901888 bytes  (12582912 KiB) (12288 MiB) (12  GiB)
#
# A larger merkle depth also allows more concurrency when processing many
# concurrent updates
# **REQUIRED**
merkleDepth: 19

# The peer list specifies a list of other database nodes that are in the same
# cluster as this node. This database node will contiually try to connect to
# and sync with the nodes in this list. Alternatively peers can be added at
# runtime if an authorized client requests that the node connect to another 
# node.
# **REQUIRED**
peers:
# Uncomment these next lines if there are other peers in the cluster to connect
# to and edit accordingly
#    - id: WWRL000001
#      host: 127.0.0.1
#      port: 9191
#    - id: WWRL000002
#      host: 127.0.0.1
#      port: 9292

# These are the possible log levels in order from lowest to highest level.
# Specifying a particular log level means you will see all messages at that
# level and below. For example, if debug is specified, all log messages will
# be seen. If no level is specified or if the log level specified is not valid
# then the level defaults to the error level
# critical
# error
# warning
# notice
# info
# debug
logLevel: info

# This field can be used to specify a devicedb cloud node to which to connect
# If omitted then no cloud connection is established.
# cloud:
#     # noValidate is a flag specifying whether or not to validate the cloud
#     # node's TLS certificate chain. If omitted this field defaults to false
#     # Setting this field to true is not reccomended in production. It can
#     # be useful, however, when running against a test cloud where self-signed
#     # certificates are used.
#     noValidate: true
#     # The id field is used to verify the host name that the cloud server provides
#     # in its TLS certificate chain. If this field is omitted then the host field
#     # will be used as the expected host name in the cloud's certificate. If
#     # noValidate is true then no verification is performed either way so this
#     # effectively ignored. In this example, the TLS certificate uses a wildcard
#     # certificate so the server name provided in the certificate will not 
#     # match the domain name of the host to which this node is connecting.
#     id: *.wigwag.com
#     host: devicedb.wigwag.com
#     port: 443

# The TLS options specify file paths to PEM encoded SSL certificates and keys
# All connections between database nodes use TLS to identify and authenticate
# each other. A single certificate and key can be used if that certificate has
# the server and client extended key usage options enabled. If seperate
# certificates are used for the client and server certificates then the common
# name on the clint and server certificate must match. The common name of the
# certificate is used to identify this database node with other database nodes
# The rootCA file is the root certificate chain that was used to generate these 
# certificates and is shared between nodes in a cluster. A database client does 
# not need to provide a client certificate when sending a request to a database 
# node but does need to verify the database node's server certificate against 
# the same root certificate chain.
# **REQUIRED**
tls:
    # If using a single certificate for both client and server authentication
    # then it is specified using the certificate and key options as shown below
    # If using seperate client and server certificates then uncomment the options
    # below for clientCertificate, clientKey, serverCertificate, and serverKey
    
    # A PEM encoded certificate with the 'server' and 'client' extendedKeyUsage 
    # options set
    certificate: path/to/cert.pem
    
    # A PEM encoded key corresponding to the specified certificate
    key: path/to/key.pem
    
    # A PEM encoded 'client' type certificate
    # clientCertificate: path/to/clientCert.pem
    
    # A PEM encoded key corresponding to the specified client certificate
    # clientKey: path/to/clientKey.pem
    
    # A PEM encoded 'server' type certificate
    # serverCertificate: path/to/serverCert.pem
    
    # A PEM encoded key corresponding to the specified server certificate
    # serverKey: path/to/serverKey.pem
    
    # A PEM encoded certificate chain that can be used to verify the previous
    # certificates
    rootCA: path/to/ca-chain.pem
`

var usage string = 
`Usage: devicedb <command> <arguments> | -version

Commands:
    start      Start a devicedb relay server
    conf       Generate a template config file for a relay server
    upgrade    Upgrade an old database to the latest format on a relay
    benchmark  Benchmark devicedb performance on a relay
    cluster    Manage a devicedb cloud cluster
    
Use devicedb help <command> for more usage information about a command.
`

var clusterUsage string = 
`Usage: devicedb cluster <cluster_command> <arguments>

Cluster Commands:
    start         Start a devicedb cloud node
    remove        Force a node to be removed from the cluster
    decommission  Migrate data away from a node then remove it from the cluster
    replace       Replace a dead node with a new node
    
Use devicedb cluster help <cluster_command> for more usage information about a cluster command.
`

var commandUsage string = "Usage: devicedb %s <arguments>\n"

func isValidPartitionCount(p uint64) bool {
    return (p != 0 && ((p & (p - 1)) == 0)) && p >= cluster.MinPartitionCount && p <= cluster.MaxPartitionCount
}

func isValidJoinAddress(s string) bool {
    _, _, err := parseJoinAddress(s)

    return err == nil
}

func parseJoinAddress(s string) (hostname string, port int, err error) {
    parts := strings.Split(s, ":")

    if len(parts) != 2 {
        err = errors.New("")

        return
    }

    port64, err := strconv.ParseUint(parts[1], 10, 16)

    if err != nil {
        return
    }

    hostname = parts[0]
    port = int(port64)

    return
}

func main() {
    startCommand := flag.NewFlagSet("start", flag.ExitOnError)
    confCommand := flag.NewFlagSet("conf", flag.ExitOnError)
    upgradeCommand := flag.NewFlagSet("upgrade", flag.ExitOnError)
    benchmarkCommand := flag.NewFlagSet("benchmark", flag.ExitOnError)
    helpCommand := flag.NewFlagSet("help", flag.ExitOnError)
    clusterStartCommand := flag.NewFlagSet("start", flag.ExitOnError)
    clusterRemoveCommand := flag.NewFlagSet("remove", flag.ExitOnError)
    clusterDecommissionCommand := flag.NewFlagSet("decommission", flag.ExitOnError)
    clusterReplaceCommand := flag.NewFlagSet("replace", flag.ExitOnError)
    clusterHelpCommand := flag.NewFlagSet("help", flag.ExitOnError)

    startConfigFile := startCommand.String("conf", "", "The config file for this server")

    upgradeLegacyDB := upgradeCommand.String("legacy", "", "The path to the legacy database data directory")
    upgradeConfigFile := upgradeCommand.String("conf", "", "The config file for this server. If this argument is used then don't use the -db and -merkle options.")
    upgradeNewDB := upgradeCommand.String("db", "", "The directory to use for the upgraded data directory")
    upgradeNewDBMerkleDepth := upgradeCommand.Uint64("merkle", uint64(0), "The merkle depth to use for the upgraded data directory. Default value will be used if omitted.")
    
    benchmarkDB := benchmarkCommand.String("db", "", "The directory to use for the benchark test scratch space")
    benchmarkMerkle := benchmarkCommand.Uint64("merkle", uint64(0), "The merkle depth to use with the benchmark databases")

    clusterStartHost := clusterStartCommand.String("host", "localhost", "HTTP The hostname or ip to listen on. This is the advertised host address for this node.")
    clusterStartPort := clusterStartCommand.Uint("port", uint(55555), "HTTP This is the intra-cluster port used for communication between nodes and between secure clients and the cluster.")
    clusterStartRelayHost := clusterStartCommand.String("relay_host", "localhost", "HTTPS The hostname or ip to listen on for incoming relay connections.")
    clusterStartRelayPort := clusterStartCommand.Uint("relay_port", uint(443), "HTTPS This is the port used for incoming relay connections.")
    clusterStartTLSCertificate := clusterStartCommand.String("cert", "", "PEM encoded x509 certificate to be used by relay connections. (Required) (Ex: /path/to/certs/cert.pem)")
    clusterStartTLSKey := clusterStartCommand.String("key", "", "PEM encoded x509 key corresponding to the specified 'cert'. (Required) (Ex: /path/to/certs/key.pem)")
    clusterStartTLSRelayCA := clusterStartCommand.String("relay_ca", "", "PEM encoded x509 certificate authority used to validate relay client certs for incoming relay connections. (Required) (Ex: /path/to/certs/relays.ca.pem)")
    clusterStartPartitions := clusterStartCommand.Uint64("partitions", uint64(cluster.DefaultPartitionCount), "The number of hash space partitions in the cluster. Must be a power of 2. (Only specified when starting a new cluster)")
    clusterStartReplicationFactor := clusterStartCommand.Uint64("replication_factor", uint64(3), "The number of replcas required for every database key. (Only specified when starting a new cluster)")
    clusterStartStore := clusterStartCommand.String("store", "", "The path to the storage. (Required) (Ex: /tmp/devicedb)")
    clusterStartJoin := clusterStartCommand.String("join", "", "Join the cluster that the node listening at this address belongs to. Ex: 10.10.102.8:80")

    clusterRemoveHost := clusterRemoveCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node removal.")
    clusterRemovePort := clusterRemoveCommand.Uint("port", uint(55555), "The port of the cluster member to contact.")
    clusterRemoveNodeID := clusterRemoveCommand.Uint64("node", uint64(0), "The ID of the node that should be removed from the cluster. Defaults to the ID of the node being contacted.")

    clusterDecommissionHost := clusterDecommissionCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node decommissioning.")
    clusterDecommissionPort := clusterDecommissionCommand.Uint("port", uint(55555), "The port of the cluster member to contact.")
    clusterDecommissionNodeID := clusterDecommissionCommand.Uint64("node", uint64(0), "The ID of the node that should be decommissioned from the cluster. Defaults to the ID of the node being contacted.")

    clusterReplaceHost := clusterReplaceCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node decommissioning.")
    clusterReplacePort := clusterReplaceCommand.Uint("port", uint(55555), "The port of the cluster member to contact.")
    clusterReplaceNodeID := clusterReplaceCommand.Uint64("node", uint64(0), "The ID of the node that is being replaced. Defaults the the ID of the node being contacted.")
    clusterReplaceReplacementNodeID := clusterReplaceCommand.Uint64("replacement_node", uint64(0), "The ID of the node that is replacing the other node.")

    if len(os.Args) < 2 {
        fmt.Fprintf(os.Stderr, "Error: %s", "No command specified\n\n")
        fmt.Fprintf(os.Stderr, "%s", usage)
        os.Exit(1)
    }

    switch os.Args[1] {
    case "cluster":
        if len(os.Args) < 3 {
            fmt.Fprintf(os.Stderr, "Error: %s", "No cluster command specified\n\n")
            fmt.Fprintf(os.Stderr, "%s", clusterUsage)
            os.Exit(1)
        }

        switch os.Args[2] {
        case "start":
            clusterStartCommand.Parse(os.Args[3:])
        case "remove":
            clusterRemoveCommand.Parse(os.Args[3:])
        case "decommission":
            clusterDecommissionCommand.Parse(os.Args[3:])
        case "replace":
            clusterReplaceCommand.Parse(os.Args[3:])
        case "help":
            clusterHelpCommand.Parse(os.Args[3:])
        case "-help":
            fmt.Fprintf(os.Stderr, "%s", clusterUsage)
        default:
            fmt.Fprintf(os.Stderr, "Error: \"%s\" is not a recognized cluster command\n\n", os.Args[2])
            fmt.Fprintf(os.Stderr, "%s", clusterUsage)
            os.Exit(1)
        }
    case "start":
        startCommand.Parse(os.Args[2:])
    case "conf":
        confCommand.Parse(os.Args[2:])
    case "upgrade":
        upgradeCommand.Parse(os.Args[2:])
    case "benchmark":
        benchmarkCommand.Parse(os.Args[2:])
    case "help":
        helpCommand.Parse(os.Args[2:])
    case "-help":
        fmt.Fprintf(os.Stderr, "%s", usage)
        os.Exit(0)
    case "-version":
        fmt.Fprintf(os.Stdout, "%s\n", DEVICEDB_VERSION)
        os.Exit(0)
    default:
        fmt.Fprintf(os.Stderr, "Error: \"%s\" is not a recognized command\n\n", os.Args[1])
        fmt.Fprintf(os.Stderr, "%s", usage)
        os.Exit(1)
    }

    if startCommand.Parsed() {
        if *startConfigFile == "" {
            fmt.Fprintf(os.Stderr, "Error: No config file specified\n")
            os.Exit(1)
        }

        start(*startConfigFile)
    }

    if confCommand.Parsed() {
        fmt.Fprintf(os.Stderr, "%s", templateConfig)
        os.Exit(0)
    }

    if upgradeCommand.Parsed() {
        var serverConfig YAMLServerConfig
        
        if len(*upgradeConfigFile) != 0 {
            err := serverConfig.LoadFromFile(*upgradeConfigFile)
            
            if err != nil {
                fmt.Fprintf(os.Stderr, "Error: Unable to load configuration file: %v\n", err)
                os.Exit(1)
            }
        } else {
            if *upgradeNewDBMerkleDepth < uint64(MerkleMinDepth) || *upgradeNewDBMerkleDepth > uint64(MerkleMaxDepth) {
                fmt.Fprintf(os.Stderr, "No valid merkle depth specified. Defaulting to %d\n", MerkleDefaultDepth)
                
                serverConfig.MerkleDepth = MerkleDefaultDepth
            } else {
                serverConfig.MerkleDepth = uint8(*upgradeNewDBMerkleDepth)
            }
            
            if len(*upgradeNewDB) == 0 {
                fmt.Fprintf(os.Stderr, "Error: No database directory (-db) specified\n")
                os.Exit(1)
            }
            
            serverConfig.DBFile = *upgradeNewDB
        }
        
        if len(*upgradeLegacyDB) == 0 {
            fmt.Fprintf(os.Stderr, "Error: No legacy database directory (-legacy) specified\n")
            os.Exit(1)
        }
        
        err := UpgradeLegacyDatabase(*upgradeLegacyDB, serverConfig)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Unable to migrate legacy database: %v\n", err)
            os.Exit(1)
        }
    }

    if benchmarkCommand.Parsed() {
        var benchmarkMagnitude int = 10000
        var serverConfig ServerConfig
        var server *Server

        if len(*benchmarkDB) == 0 {
            fmt.Fprintf(os.Stderr, "Error: No database directory (-db) specified\n")
            os.Exit(1)
        }
        
        if *benchmarkMerkle < uint64(MerkleMinDepth) || *benchmarkMerkle > uint64(MerkleMaxDepth) {
            fmt.Fprintf(os.Stderr, "No valid merkle depth specified. Defaulting to %d\n", MerkleDefaultDepth)
            
            serverConfig.MerkleDepth = MerkleDefaultDepth
        } else {
            serverConfig.MerkleDepth = uint8(*benchmarkMerkle)
        }
        
        err := os.RemoveAll(*benchmarkDB)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Unable to initialized benchmark workspace at %s: %v\n", *benchmarkDB, err)
            os.Exit(1)
        }
        
        SetLoggingLevel("error")
        serverConfig.DBFile = *benchmarkDB
        server, err = NewServer(serverConfig)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Unable to initialize test databas: %v\n", err)
            os.Exit(1)
        }
        
        err = benchmarkSequentialReads(benchmarkMagnitude, server)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Failed at sequential reads benchmark: %v\n", err)
            os.Exit(1)
        }
        
        err = benchmarkRandomReads(benchmarkMagnitude, server)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Failed at random reads benchmark: %v\n", err)
            os.Exit(1)
        }
        
        err = benchmarkWrites(benchmarkMagnitude, server)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Failed at writes benchmark: %v\n", err)
            os.Exit(1)
        }
    }

    if helpCommand.Parsed() {
        if len(os.Args) < 3 {
            fmt.Fprintf(os.Stderr, "Error: No command specified for help\n")
            os.Exit(1)
        }
        
        var flagSet *flag.FlagSet

        switch os.Args[2] {
        case "start":
            flagSet = startCommand
        case "conf":
            fmt.Fprintf(os.Stderr, "Usage: devicedb conf\n")
            os.Exit(0)
        case "upgrade":
            flagSet = upgradeCommand
        case "benchmark":
            flagSet = benchmarkCommand
        case "cluster":
            fmt.Fprintf(os.Stderr, commandUsage, "cluster <cluster_command>")
            os.Exit(0)
        default:
            fmt.Fprintf(os.Stderr, "Error: \"%s\" is not a valid command.\n", os.Args[2])
            os.Exit(1)
        }

        fmt.Fprintf(os.Stderr, commandUsage + "\n", os.Args[2])
        flagSet.PrintDefaults()
        os.Exit(0)
    }

    if clusterStartCommand.Parsed() {
        if *clusterStartJoin != "" && !isValidJoinAddress(*clusterStartJoin) {
            fmt.Fprintf(os.Stderr, "Error: -join must specify a valid address of some host in an existing cluster formatted like: host:port Ex: 10.10.102.89:80.\n")
            os.Exit(1)
        }

        if *clusterStartStore == "" {
            fmt.Fprintf(os.Stderr, "Error: -store is a required parameter of the devicedb cluster start command. It must specify a valid file system path.\n")
            os.Exit(1)
        }

        if *clusterStartJoin == "" {
            if !isValidPartitionCount(*clusterStartPartitions) {
                fmt.Fprintf(os.Stderr, "Error: -partitions must be a power of 2 and be in the range [%d, %d]\n", cluster.MinPartitionCount, cluster.MaxPartitionCount)
                os.Exit(1)
            }

            if *clusterStartReplicationFactor == 0 {
                fmt.Fprintf(os.Stderr, "Error: -replication_factor must be a positive value")
                os.Exit(1)
            }
        }

        if *clusterStartTLSCertificate == "" {
            fmt.Fprintf(os.Stderr, "Error: -cert must be specified\n")
            os.Exit(1)
        }

        if *clusterStartTLSKey == "" {
            fmt.Fprintf(os.Stderr, "Error: -key must be specified\n")
            os.Exit(1)
        }

        if *clusterStartTLSRelayCA  == "" {
            fmt.Fprintf(os.Stderr, "Error: -relay_ca must be specified\n")
            os.Exit(1)
        }

        certificate, err := ioutil.ReadFile(*clusterStartTLSCertificate)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Could not the TLS certificate from %s: %v\n", *clusterStartTLSCertificate, err.Error())
            os.Exit(1)
        }

        key, err := ioutil.ReadFile(*clusterStartTLSKey)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Could not the TLS key from %s: %v\n", *clusterStartTLSKey, err.Error())
            os.Exit(1)
        }

        relayCA, err := ioutil.ReadFile(*clusterStartTLSRelayCA)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: Could not the TLS Relay CA from %s: %v\n", *clusterStartTLSRelayCA, err.Error())
            os.Exit(1)
        }

        cert, err := tls.X509KeyPair([]byte(certificate), []byte(key))
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: The specified certificate and key represent an invalid public/private key pair")
            os.Exit(1)
        }

        rootCAs := x509.NewCertPool()

        if !rootCAs.AppendCertsFromPEM([]byte(relayCA)) {
            fmt.Fprintf(os.Stderr, "Error: The specified TLS Relay CA was not valid")
            os.Exit(1)
        }

        var seedHost string
        var seedPort int
        var startOptions node.NodeInitializationOptions

        if *clusterStartJoin != "" {
            seedHost, seedPort, _ = parseJoinAddress(*clusterStartJoin)
            startOptions.JoinCluster = true
            startOptions.SeedNodeHost = seedHost
            startOptions.SeedNodePort = seedPort
        } else {
            startOptions.StartCluster = true
            startOptions.ClusterSettings.Partitions = *clusterStartPartitions
            startOptions.ClusterSettings.ReplicationFactor = *clusterStartReplicationFactor
        }

        startOptions.ClusterHost = *clusterStartHost
        startOptions.ClusterPort = int(*clusterStartPort)
        startOptions.ExternalHost = *clusterStartRelayHost
        startOptions.ExternalPort = int(*clusterStartRelayPort)

        cloudNodeStorage := storage.NewLevelDBStorageDriver(*clusterStartStore, nil)
        cloudServer := NewCloudServer(CloudServerConfig{
            InternalHost: *clusterStartHost,
            InternalPort: int(*clusterStartPort),
            ExternalHost: *clusterStartRelayHost,
            ExternalPort: int(*clusterStartRelayPort),
            NodeID: 1,
            RelayTLSConfig: &tls.Config{
                Certificates: []tls.Certificate{ cert },
                ClientCAs: rootCAs,
                ClientAuth: tls.RequireAndVerifyClientCert,
            },
        })
        cloudNode := node.New(node.ClusterNodeConfig{
            CloudServer: cloudServer,
            StorageDriver: cloudNodeStorage,
            MerkleDepth: 4,
            Capacity: 1,
        })

        if err := cloudNode.Start(startOptions); err != nil {
            os.Exit(1)
        }

        os.Exit(0)
    }

    if clusterRemoveCommand.Parsed() {
        fmt.Fprintf(os.Stderr, "Removing node %d from the cluster...\n", *clusterRemoveNodeID)
        client := NewClient(ClientConfig{ })
        err := client.ForceRemoveNode(context.TODO(), PeerAddress{ Host: *clusterRemoveHost, Port: int(*clusterRemovePort) }, *clusterRemoveNodeID)

        if err != nil {
            Log.Errorf("Error: Unable to remove node %d from the cluster: %v", *clusterRemoveNodeID, err.Error())

            os.Exit(1)
        }

        fmt.Fprintf(os.Stderr, "Removed node %d from the cluster.\n", *clusterRemoveNodeID)

        os.Exit(0)
    }

    if clusterDecommissionCommand.Parsed() {
        fmt.Fprintf(os.Stderr, "Asking node at %s on port %d to decommission node %d\n", *clusterDecommissionHost, *clusterDecommissionPort, *clusterDecommissionNodeID)
        os.Exit(1)

    }

    if clusterReplaceCommand.Parsed() {
        if *clusterReplaceReplacementNodeID == 0 {
            fmt.Fprintf(os.Stderr, "Error: -replacement_node must be specified\n")
            os.Exit(1)
        }

        fmt.Fprintf(os.Stderr, "Asking node at %s on port %d to replace node %d with node %d\n", *clusterReplaceHost, *clusterReplacePort, *clusterReplaceNodeID, *clusterReplaceReplacementNodeID)
        os.Exit(1)
    }

    if clusterHelpCommand.Parsed() {
        if len(os.Args) < 4 {
            fmt.Fprintf(os.Stderr, "Error: No cluster command specified for help\n")
            os.Exit(1)
        }
        
        var flagSet *flag.FlagSet

        switch os.Args[3] {
        case "start":
            flagSet = clusterStartCommand
        case "remove":
            flagSet = clusterRemoveCommand
        case "decommission":
            flagSet = clusterDecommissionCommand 
        case "replace":
            flagSet = clusterReplaceCommand
        default:
            fmt.Fprintf(os.Stderr, "Error: \"%s\" is not a valid cluster command.\n", os.Args[3])
            os.Exit(1)
        }

        fmt.Fprintf(os.Stderr, commandUsage + "\n", "cluster " + os.Args[3])
        flagSet.PrintDefaults()
        os.Exit(0)
    }
}

func start(configFile string) {
    var sc ServerConfig
        
    err := sc.LoadFromFile(configFile)

    if err != nil {
        fmt.Fprintf(os.Stderr, "Unable to load config file: %s\n", err.Error())
        
        return
    }

    server, err := NewServer(sc)

    if err != nil {
        fmt.Fprintf(os.Stderr, "Unable to create server: %s\n", err.Error())
        
        return
    }

    sc.Hub.SyncController().Start()
    sc.Hub.StartForwardingEvents()
    sc.Hub.StartForwardingAlerts()
    server.StartGC()

    server.Start()
}

// test reads per second
func benchmarkSequentialReads(benchmarkMagnitude int, server *Server) error {
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench1" + RandomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(RandomString() + RandomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
        _, err := server.Buckets().Get("default").Batch(updateBatch)
        
        if err != nil {
            return err
        }
    }
    
    iter, err := server.Buckets().Get("default").GetMatches([][]byte{ []byte("key") })
    
    if err != nil {
        return err
    }
    
    defer iter.Release()

    start := time.Now()
    
    for iter.Next() {
    }
    
    if iter.Error() != nil {
        return iter.Error()
    }
    
    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    readsPerSecond := time.Second / average
    
    fmt.Printf("%d sequential reads took %s or an average of %s per read or %d reads per second\n", benchmarkMagnitude, elapsed.String(), average.String(), readsPerSecond)
    
    return nil
}

func benchmarkRandomReads(benchmarkMagnitude int, server *Server) error {
    keys := make([]string, 0, benchmarkMagnitude)
    
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench2" + RandomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(RandomString() + RandomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
        _, err := server.Buckets().Get("default").Batch(updateBatch)
        
        if err != nil {
            return err
        }
        
        keys = append(keys, string(key))
    }

    start := time.Now()
    
    for _, key := range keys {
        _, err := server.Buckets().Get("default").Get([][]byte{ []byte(key) })
        
        if err != nil {
            return err
        }
    }
    
    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    readsPerSecond := time.Second / average
    
    fmt.Printf("%d random reads took %s or an average of %s per read or %d reads per second\n", benchmarkMagnitude, elapsed.String(), average.String(), readsPerSecond)
    
    return nil
}

func benchmarkWrites(benchmarkMagnitude int, server *Server) error {
    var batchWaits sync.WaitGroup
    var err error
    
    start := time.Now()
    
    // Seed database for test
    for i := 0; i < benchmarkMagnitude; i += 1 {
        key := []byte("keyBench3" + RandomString())
        updateBatch := NewUpdateBatch()
        updateBatch.Put(key, []byte(RandomString() + RandomString()), NewDVV(NewDot("", 0), map[string]uint64{ }))
    
        batchWaits.Add(1)
        
        go func() {
            _, e := server.Buckets().Get("default").Batch(updateBatch)
            
            if e != nil {
                err = e
            }
            
            batchWaits.Done()
        }()
    }
    
    batchWaits.Wait()
    
    if err != nil {
        return err
    }

    elapsed := time.Since(start)
    average := time.Duration(elapsed.Nanoseconds() / int64(benchmarkMagnitude))
    batchesPerSecond := time.Second / average
    
    fmt.Printf("%d writes took %s or an average of %s per write or %d writes per second\n", benchmarkMagnitude, elapsed.String(), average.String(), batchesPerSecond)
    
    return nil
}