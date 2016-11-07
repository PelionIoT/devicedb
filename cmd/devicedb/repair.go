package main

import (
    "fmt"
    
    //. "devicedb"
)

func init() {
    registerCommand("repair", repairDatabase, repairUsage)
}

var repairUsage string = 
`Usage: devicedb repair -db=[database directory]
`

func repairDatabase() {
    fmt.Println("TODO")
}