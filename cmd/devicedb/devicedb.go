// devicedb cluster start-node -conf=/a/b/c.yaml -seed=true -partitions=256 -replication_factor=3  Create a new single node cluster
// devicedb cluster start-node -conf=/a/b/c.yaml                                   Create a node that does not belong to a cluster yet
// devicedb cluster add-node [CLUSTER_ADDRESS] [NODE_ADDRESS]                      Add a node to an existing cluster
// devicedb cluster remove-node [CLUSTER_ADDRESS] [NODE_ID]                        Forcefully remove a node from the cluster and reassign its tokens
// devicedb cluster decommission-node [CLUSTER_ADDRESS] [NODE_ID]                  Gracefully remove a node from the cluster
// devicedb cluster replace-node [CLUSTER_ADDRESS] [NODE_ID] [REPLACEMENT_ADDRESS]
// devicedb cluster status [CLUSTER_ADDRESS]                                       Report information related to cluster health and provisioning status
// devicedb cluster partitions [CLUSTER_ADDRESS]                                   Report cluster membership info and info about data distribution in the cluster
// devicedb cluster performance [CLUSTER_ADDRESS]                                  Report information related to server load on the various nodes and usage statistics
// devicedb cluster conf                                                           Generate a template config file for a node

package main

import (
    "fmt"
    "flag"
    "os"
    . "devicedb/version"
)

var usage string = 
`Usage: devicedb [ command [arguments] ] | -version

Commands:
    start      Start a devicedb server
    conf       Generate a template config file
    upgrade    Upgrade an old database to the latest format
    benchmark  Benchmark devicedb performance
    cluster    Manage a devicedb cloud cluster
    
Use devicedb help [command] for more usage information about a command.
`

type Command struct {
    Execute func()
    Usage string
}

var optConfigFile *string
var optLegacyDir *string
var optDatabaseDir *string
var optMerkleDepth *uint64
var commands map[string]Command

func registerCommand(name string, execute func(), usage string) {
    if commands == nil {
        commands = make(map[string]Command)
    }
    
    commands[name] = Command{
        Execute: execute,
        Usage: usage,
    }
}

func init() {
    optConfigFile = flag.String("conf", "", "Config file to use in the server")
    optLegacyDir = flag.String("legacy", "", "Legacy database directory")
    optDatabaseDir = flag.String("db", "", "Datase directory")
    optMerkleDepth = flag.Uint64("merkle", 0, "Merkle depth")
    
    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, usage)
    }
}

func main() {
    if len(os.Args) < 2 {
        flag.Usage()
        
        return
    }
    
    commandName := os.Args[1]
    os.Args = os.Args[1:]

    flag.Parse()
    
    if command, ok := commands[commandName]; ok {
        command.Execute()
    } else if commandName == "help" {
        helpTopic := flag.Arg(0)
        
        if command, ok := commands[helpTopic]; ok {
            fmt.Fprintf(os.Stderr, command.Usage)
        } else {
            flag.Usage()
        }
    } else if commandName == "-version" {
        fmt.Fprintf(os.Stdout, "%s\n", DEVICEDB_VERSION)
    } else {
        flag.Usage()
    }
}
