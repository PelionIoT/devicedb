package main

import (
    "fmt"
    "flag"
    "os"    
)

var usage string = 
`Usage: devicedb command [arguments]

Commands:
    start     Start a devicedb server
    conf      Generate a template config file
    port      Port an old database to the latest format
    repair    Port an old database to the latest format
    
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
    } else {
        flag.Usage()
    }
}
