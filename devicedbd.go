package main

import (
    . "devicedb/io"
)

func main() {
    server, _ := NewServer("/tmp/testdb")
    server.Start()
}