package main

import (
    "fmt"
    "os"
        
    . "devicedb/compatibility"
    . "devicedb/shared"
)

func init() {
    registerCommand("upgrade", upgradeLegacyDatabase, upgradeLegacyDatabaseUsage)
}

var upgradeLegacyDatabaseUsage string = 
`Usage: devicedb upgrade -legacy=[legacy database directory] [-conf=[config file] | -db=[new database directory] -merkle=[new database merkle depth] ]
`

func upgradeLegacyDatabase() {
    var serverConfig YAMLServerConfig
    
    if len(*optConfigFile) != 0 {
        err := serverConfig.LoadFromFile(*optConfigFile)
        
        if err != nil {
            fmt.Fprintf(os.Stderr, "Unable to load configuration file: %v\n", err)
            
            return
        }
    } else {
        if *optMerkleDepth < uint64(MerkleMinDepth) || *optMerkleDepth > uint64(MerkleMaxDepth) {
            fmt.Fprintf(os.Stderr, "No valid merkle depth specified. Defaulting to %d\n", MerkleDefaultDepth)
            
            serverConfig.MerkleDepth = MerkleDefaultDepth
        } else {
            serverConfig.MerkleDepth = uint8(*optMerkleDepth)
        }
        
        if len(*optDatabaseDir) == 0 {
            fmt.Fprintf(os.Stderr, "No database directory (-db) specified\n")
            
            return
        }
        
        serverConfig.DBFile = *optDatabaseDir
    }
    
    if len(*optLegacyDir) == 0 {
        fmt.Fprintf(os.Stderr, "No legacy database directory (-legacy) specified\n")
        
        return
    }
    
    err := UpgradeLegacyDatabase(*optLegacyDir, serverConfig)
    
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error migrating legacy database: %v\n", err)
    }
}