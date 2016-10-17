package io_test

import (
	. "devicedb/io"
    
    "time"
    //"devicedb/storage"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sync", func() {
    Describe("InitiatorSyncSession", func() {
        Describe("#NextState", func() {
            var server1 *Server
            var server2 *Server
            stop1 := make(chan int)
            stop2 := make(chan int)
            
            BeforeEach(func() {
                server1, _ = NewServer("/tmp/testdb-" + randomString())
                server2, _ = NewServer("/tmp/testdb-" + randomString())
                
                go func() {
                    server1.Start()
                    stop1 <- 1
                }()
                
                go func() {
                    server2.Start()
                    stop2 <- 1
                }()
                
                time.Sleep(time.Millisecond * 500)
            })
            
            AfterEach(func() {
                server1.Stop()
                server2.Stop()
                <-stop1
                <-stop2
            })
            
            It("(START -> HANDSHAKE) + (START -> HASH_COMPARE)", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                responderSyncSession := NewResponderSyncSession(server2.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(START)
                responderSyncSession.SetState(START)
                
                req := initiatorSyncSession.NextState(nil)
                res := responderSyncSession.NextState(req)
                
                Expect(req.MessageType).Should(Equal(SYNC_START))
                Expect(res.MessageType).Should(Equal(SYNC_START))
                Expect(initiatorSyncSession.State()).Should(Equal(HANDSHAKE))
                Expect(responderSyncSession.State()).Should(Equal(HASH_COMPARE))
            })
        })
    })
})
