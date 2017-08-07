// devicedb cluster start 
//   -host <ip> (
//   -port <port> (port to bind to. Also the advertised port for this node)
//   -partitions <p> (only if not using -join)
//   -replication_factor <rf> (only if not using -join)
//   -store <store_path>  
//   -join <seed_address>
// devicedb cluster remove 
//   -host <host>
//   -port <port>
//   -node_id <node id>
// devicedb cluster decommission 
//   -host <host>
//   -port <port>
// devicedb cluster replace 
//   -host <host>
//   -port <port>
//   -node_id <node id>

package main

import (
    "flag"
    "fmt"
    "os"
    "devicedb/cluster"
    "strings"
    "strconv"
    "errors"
)

var usage string = 
`Usage: devicedb <command> <arguments> | -version

Commands:
    start      Start a devicedb server
    conf       Generate a template config file
    upgrade    Upgrade an old database to the latest format
    benchmark  Benchmark devicedb performance
    cluster    Manage a devicedb cloud cluster
    
Use devicedb help [command] for more usage information about a command.
`

var clusterUsage string = 
`Usage: devicedb cluster <cluster_command> <arguments>

Cluster Commands:
    start        Start a devicedb server
    remove       Force a node to be removed from the cluster
    decomission  Migrate data away from a node then remove it from the cluster
    replace      Replace a dead node with a new node
    
Use devicedb cluster help [command] for more usage information about a cluster command.
`

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
    clusterStartCommand := flag.NewFlagSet("start", flag.ExitOnError)
    clusterRemoveCommand := flag.NewFlagSet("remove", flag.ExitOnError)
    clusterDecommissionCommand := flag.NewFlagSet("decommission", flag.ExitOnError)
    clusterReplaceCommand := flag.NewFlagSet("replace", flag.ExitOnError)

    //clusterStartHost := clusterStartCommand.String("host", "localhost", "The hostname or ip to listen on. This is the advertised host address for this node.")
    //clusterStartPort := clusterStartCommand.Uint("port", uint(80), "The port to bind to.")
    clusterStartPartitions := clusterStartCommand.Uint64("partitions", uint64(cluster.DefaultPartitionCount), "The number of hash space partitions in the cluster. Must be a power of 2. (Only specified when starting a new cluster)")
    clusterStartReplicationFactor := clusterStartCommand.Uint64("replication_factor", uint64(3), "The number of replcas required for every database key. (Only specified when starting a new cluster)")
    clusterStartStore := clusterStartCommand.String("store", "", "The path to the storage. (Required) (Ex: /tmp/devicedb)")
    clusterStartJoin := clusterStartCommand.String("join", "", "Join the cluster that the node listening at this address belongs to. Ex: 10.10.102.8:80")

    /*clusterRemoveHost := clusterRemoveCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node removal.")
    clusterRemovePort := clusterRemoveCommand.Uint("port", uint(80), "The port of the cluster member to contact.")
    clusterRemoveNodeID := clusterRemoveCommand.Uint64("node", uint64(0), "The ID of the node that should be removed from the cluster. Defaults to the ID of the node being contacted.")

    clusterDecommissionHost := clusterRemoveCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node decommissioning.")
    clusterDecommissionPort := clusterRemoveCommand.Uint("port", uint(80), "The port of the cluster member to contact.")
    clusterDecommissionNodeID := clusterRemoveCommand.Uint64("node", uint64(0), "The ID of the node that should be decommissioned from the cluster. Defaults to the ID of the node being contacted.")

    clusterReplaceHost := clusterRemoveCommand.String("host", "localhost", "The hostname or ip of some cluster member to contact to initiate the node decommissioning.")
    clusterReplacePort := clusterRemoveCommand.Uint("port", uint(80), "The port of the cluster member to contact.")
    clusterReplaceNodeID := clusterRemoveCommand.Uint64("node", uint64(0), "The ID of the node that is being replaced. Defaults the the ID of the node being contacted.")*/
    clusterReplaceReplacementNodeID := clusterRemoveCommand.Uint64("replacement_node", uint64(0), "The ID of the node that is replacing the other node.")

    if len(os.Args) < 2 {
        fmt.Fprintf(os.Stderr, "%s", usage)
        os.Exit(1)
    }

    switch os.Args[1] {
    case "cluster":
        if len(os.Args) < 3 {
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
        case "-help":
            fmt.Fprintf(os.Stderr, "%s", clusterUsage)
        default:
            fmt.Fprintf(os.Stderr, "%s", "Usage: devicedb cluster <subcommand>")
            os.Exit(1)
        }
    case "start":
    case "conf":
    case "upgrade":
    case "benchmark":
    case "help":
    default:
        flag.PrintDefaults()
        os.Exit(1)
    }

    if clusterStartCommand.Parsed() {
        if *clusterStartJoin != "" && !isValidJoinAddress(*clusterStartJoin) {
            fmt.Fprintf(os.Stderr, "-join must specify a valid address of some host in an existing cluster formatted like: host:port Ex: 10.10.102.89:80.\n")
            os.Exit(1)
        }

        if *clusterStartStore == "" {
            fmt.Fprintf(os.Stderr, "-store is a required parameter of the devicedb cluster start command. It must specify a valid file system path.\n")
            os.Exit(1)
        }

        if *clusterStartJoin == "" {
            if !isValidPartitionCount(*clusterStartPartitions) {
                fmt.Fprintf(os.Stderr, "-partitions must be a power of 2 and be in the range [%d, %d]\n", cluster.MinPartitionCount, cluster.MaxPartitionCount)
                os.Exit(1)
            }

            if *clusterStartReplicationFactor == 0 {
                fmt.Fprintf(os.Stderr, "-replication_factor must be a positive value")
                os.Exit(1)
            }
        }
    }

    if clusterRemoveCommand.Parsed() {
    }

    if clusterDecommissionCommand.Parsed() {
    }

    if clusterReplaceCommand.Parsed() {
        if *clusterReplaceReplacementNodeID == 0 {
            fmt.Fprintf(os.Stderr, "-replacement_node must be specified")
            os.Exit(1)
        }
    }
}