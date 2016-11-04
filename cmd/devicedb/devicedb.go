package main

import (
    "fmt"
    "flag"
    
    . "devicedb"
)

var configFile *string

func init() {
    configFile = flag.String("conf", "", "Config file to use in the server")
}

func main() {
    flag.Parse()
    
    var sc ServerConfig
    
    err := sc.LoadFromFile(*configFile)
    
    if err != nil {
        fmt.Printf("Unable to load config file: %s\n", err.Error())
        
        return
    }
    
    server, err := NewServer(sc)
    
    if err != nil {
        fmt.Printf("Unable to create server: %s\n", err.Error())
        
        return
    }
    
    sc.Peer.SyncController().Start()
    
    server.Start()
}