package main

import (
    "fmt"
    
    . "devicedb"
)

func init() {
    registerCommand("start", startServer, startUsage)
}

var startUsage string = 
`Usage: devicedb start -conf=[config file]
`

func startServer() {
    var sc ServerConfig
        
    err := sc.LoadFromFile(*optConfigFile)

    if err != nil {
        fmt.Printf("Unable to load config file: %s\n", err.Error())
        
        return
    }

    server, err := NewServer(sc)

    if err != nil {
        fmt.Printf("Unable to create server: %s\n", err.Error())
        
        return
    }

    sc.Hub.SyncController().Start()
    sc.Hub.StartForwardingEvents()
    server.StartGC()

    server.Start()
}