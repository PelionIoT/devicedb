package devicedb_test

import (
    "time"
    
	. "devicedb"

	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
    It("start should block until stop is called", func() {
        server := NewServer()
        ch := make(chan int)
        
        go func() {
            server.Start()
            ch <- 1
        }()
    
        go func() {
            time.Sleep(time.Second)
            
            server.Stop()
        }()
        
        <-ch
    })
    
    Describe("POST /{bucket}/values", func() {
    })
})
