package io_test

import (
	. "devicedb/io"
	. "devicedb/dbobject"
    
    "time"
    //"devicedb/storage"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sync", func() {
    Describe("InitiatorSyncSession", func() {
        Describe("#NextState", func() {
            var server1 *Server
            stop1 := make(chan int)
            
            BeforeEach(func() {
                server1, _ = NewServer("/tmp/testdb-" + randomString())
                
                go func() {
                    server1.Start()
                    stop1 <- 1
                }()
                
                time.Sleep(time.Millisecond * 200)
            })
            
            AfterEach(func() {
                server1.Stop()
                <-stop1
            })
            
            It("START -> HANDSHAKE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(START)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_START))
                Expect(req.MessageBody.(Start).ProtocolVersion).Should(Equal(PROTOCOL_VERSION))
                Expect(req.MessageBody.(Start).MerkleDepth).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().Depth()))
                Expect(req.MessageBody.(Start).Bucket).Should(Equal("default"))
                Expect(initiatorSyncSession.State()).Should(Equal(HANDSHAKE))
            })
            
            It("HANDSHAKE -> ROOT_HASH_COMPARE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(HANDSHAKE)
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_START,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: 50,
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNode, 50)))
                Expect(req.MessageBody.(MerkleNodeHash).HashHigh).Should(Equal(rootHash.High()))
                Expect(req.MessageBody.(MerkleNodeHash).HashLow).Should(Equal(rootHash.Low()))
                Expect(initiatorSyncSession.State()).Should(Equal(ROOT_HASH_COMPARE))
                Expect(initiatorSyncSession.ResponderDepth()).Should(Equal(uint8(50)))
            })
            
            It("HANDSHAKE -> END nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(HANDSHAKE)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
                Expect(initiatorSyncSession.ResponderDepth()).Should(Equal(uint8(0)))
            })
            
            It("HANDSHAKE -> END non nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(HANDSHAKE)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_PUSH_MESSAGE,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: server1.Buckets().Get("default").Node.MerkleTree().Depth(),
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
                Expect(initiatorSyncSession.ResponderDepth()).Should(Equal(uint8(0)))
            })
            
            It("ROOT_HASH_COMPARE -> END nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(ROOT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("ROOT_HASH_COMPARE -> END non nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(ROOT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_PUSH_MESSAGE,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: server1.Buckets().Get("default").Node.MerkleTree().Depth(),
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("ROOT_HASH_COMPARE -> END root hashes match", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(ROOT_HASH_COMPARE)
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNode,
                        HashHigh: rootHash.High(),
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("ROOT_HASH_COMPARE -> LEFT_HASH_COMPARE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(ROOT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(20)
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeLeftChild := server1.Buckets().Get("default").Node.MerkleTree().LeftChild(rootNode)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                rootNodeLeftChildHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNodeLeftChild)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNode,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeLeftChild, 20)))
                Expect(req.MessageBody.(MerkleNodeHash).HashHigh).Should(Equal(rootNodeLeftChildHash.High()))
                Expect(req.MessageBody.(MerkleNodeHash).HashLow).Should(Equal(rootNodeLeftChildHash.Low()))
                Expect(initiatorSyncSession.State()).Should(Equal(LEFT_HASH_COMPARE))
            })
            
            It("ROOT_HASH_COMPARE -> DB_OBJECT_PUSH", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(ROOT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(1)
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNode,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_OBJECT_NEXT))
                Expect(req.MessageBody.(ObjectNext).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNode, 1)))
                Expect(initiatorSyncSession.State()).Should(Equal(DB_OBJECT_PUSH))
            })
            
            It("LEFT_HASH_COMPARE -> END nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(LEFT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("LEFT_HASH_COMPARE -> END non nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(LEFT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_PUSH_MESSAGE,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: server1.Buckets().Get("default").Node.MerkleTree().Depth(),
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("LEFT_HASH_COMPARE -> RIGHT_HASH_COMPARE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeRightChild := server1.Buckets().Get("default").Node.MerkleTree().RightChild(rootNode)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                initiatorSyncSession.SetState(LEFT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(4)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNode,
                        HashHigh: rootHash.High(),
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeRightChild, 4)))
                Expect(initiatorSyncSession.State()).Should(Equal(RIGHT_HASH_COMPARE))
            })
            
            It("LEFT_HASH_COMPARE -> LEFT_HASH_COMPARE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeLeftChild := server1.Buckets().Get("default").Node.MerkleTree().LeftChild(rootNode)
                rootNodeLeftLeftChild := server1.Buckets().Get("default").Node.MerkleTree().LeftChild(rootNodeLeftChild)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                initiatorSyncSession.SetState(LEFT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(4)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNodeLeftChild,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeLeftLeftChild, 4)))
                Expect(initiatorSyncSession.State()).Should(Equal(LEFT_HASH_COMPARE))
                Expect(initiatorSyncSession.CurrentNode()).Should(Equal(rootNodeLeftChild))
            })
            
            It("LEFT_HASH_COMPARE -> DB_OBJECT_PUSH", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeLeftChild := server1.Buckets().Get("default").Node.MerkleTree().LeftChild(rootNode)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                initiatorSyncSession.SetState(LEFT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(2)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNodeLeftChild,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_OBJECT_NEXT))
                Expect(req.MessageBody.(ObjectNext).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeLeftChild, 2)))
                Expect(initiatorSyncSession.State()).Should(Equal(DB_OBJECT_PUSH))
                Expect(initiatorSyncSession.CurrentNode()).Should(Equal(rootNodeLeftChild))
            })
            
            It("RIGHT_HASH_COMPARE -> END nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(RIGHT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("RIGHT_HASH_COMPARE -> END non nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(RIGHT_HASH_COMPARE)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_PUSH_MESSAGE,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: server1.Buckets().Get("default").Node.MerkleTree().Depth(),
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("RIGHT_HASH_COMPARE -> LEFT_HASH_COMPARE", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeRightChild := server1.Buckets().Get("default").Node.MerkleTree().RightChild(rootNode)
                rootNodeRightLeftChild := server1.Buckets().Get("default").Node.MerkleTree().LeftChild(rootNodeRightChild)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                initiatorSyncSession.SetState(RIGHT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(4)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNodeRightChild,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeRightLeftChild, 4)))
                Expect(initiatorSyncSession.State()).Should(Equal(LEFT_HASH_COMPARE))
                Expect(initiatorSyncSession.CurrentNode()).Should(Equal(rootNodeRightChild))
            })
            
            It("RIGHT_HASH_COMPARE -> DB_OBJECT_PUSH", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                rootNodeRightChild := server1.Buckets().Get("default").Node.MerkleTree().RightChild(rootNode)
                rootHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(rootNode)
                
                initiatorSyncSession.SetState(RIGHT_HASH_COMPARE)
                initiatorSyncSession.SetResponderDepth(2)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{
                        NodeID: rootNodeRightChild,
                        HashHigh: rootHash.High() + 1,
                        HashLow: rootHash.Low(),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_OBJECT_NEXT))
                Expect(req.MessageBody.(ObjectNext).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(rootNodeRightChild, 2)))
                Expect(initiatorSyncSession.State()).Should(Equal(DB_OBJECT_PUSH))
                Expect(initiatorSyncSession.CurrentNode()).Should(Equal(rootNodeRightChild))
            })
            
            It("DB_OBJECT_PUSH -> END nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(DB_OBJECT_PUSH)
                
                req := initiatorSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("DB_OBJECT_PUSH -> END non nil message", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                initiatorSyncSession.SetState(DB_OBJECT_PUSH)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
                Expect(initiatorSyncSession.State()).Should(Equal(END))
            })
            
            It("DB_OBJECT_PUSH -> DB_OBJECT_PUSH", func() {
                initiatorSyncSession := NewInitiatorSyncSession(123, server1.Buckets().Get("default"))
                
                rootNode := server1.Buckets().Get("default").Node.MerkleTree().RootNode()
                
                initiatorSyncSession.SetState(DB_OBJECT_PUSH)
                initiatorSyncSession.SetResponderDepth(2)
                initiatorSyncSession.SetCurrentNode(rootNode)
                
                req := initiatorSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_PUSH_MESSAGE,
                    MessageBody: PushMessage{ 
                        Key: "abc",
                        Value: NewSiblingSet(map[*Sibling]bool{ }),
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_OBJECT_NEXT))
                Expect(req.MessageBody.(ObjectNext).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(initiatorSyncSession.CurrentNode(), 2)))
                Expect(initiatorSyncSession.State()).Should(Equal(DB_OBJECT_PUSH))
            })
        })
    })
    
    Describe("ResponderSyncSession", func() {
        Describe("#NextState", func() {
            var server1 *Server
            stop1 := make(chan int)
            
            BeforeEach(func() {
                server1, _ = NewServer("/tmp/testdb-" + randomString())
                
                go func() {
                    server1.Start()
                    stop1 <- 1
                }()
                
                time.Sleep(time.Millisecond * 200)
            })
            
            AfterEach(func() {
                server1.Stop()
                <-stop1
            })
            
            It("START -> END nil message", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(START)
                
                req := responderSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("START -> END non nil message", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(START)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("START -> HASH_COMPARE", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(START)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_START,
                    MessageBody: Start{
                        ProtocolVersion: PROTOCOL_VERSION,
                        MerkleDepth: 10,
                        Bucket: "default",
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(123)))
                Expect(req.MessageType).Should(Equal(SYNC_START))
                Expect(req.MessageBody.(Start).ProtocolVersion).Should(Equal(PROTOCOL_VERSION))
                Expect(req.MessageBody.(Start).MerkleDepth).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().Depth()))
                Expect(req.MessageBody.(Start).Bucket).Should(Equal("default"))
                Expect(responderSyncSession.State()).Should(Equal(HASH_COMPARE))
                Expect(responderSyncSession.InitiatorDepth()).Should(Equal(uint8(10)))
            })
            
            It("HASH_COMPARE -> END nil message", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(HASH_COMPARE)
                
                req := responderSyncSession.NextState(nil)
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("HASH_COMPARE -> END non nil message", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(HASH_COMPARE)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_ABORT,
                    MessageBody: Abort{ },
                })
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("HASH_COMPARE -> END SYNC_NODE_HASH message with 0 node ID", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(HASH_COMPARE)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{ 
                        NodeID: 0,
                        HashHigh: 0,
                        HashLow: 0,
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("HASH_COMPARE -> END SYNC_NODE_HASH message with limit node ID", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(HASH_COMPARE)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{ 
                        NodeID: server1.Buckets().Get("default").Node.MerkleTree().NodeLimit(),
                        HashHigh: 0,
                        HashLow: 0,
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
            
            It("HASH_COMPARE -> HASH_COMPARE", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetInitiatorDepth(3)
                responderSyncSession.SetState(HASH_COMPARE)
                
                nodeID := server1.Buckets().Get("default").Node.MerkleTree().NodeLimit() - 1
                nodeHash := server1.Buckets().Get("default").Node.MerkleTree().NodeHash(nodeID)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_NODE_HASH,
                    MessageBody: MerkleNodeHash{ 
                        NodeID: nodeID,
                        HashHigh: 0,
                        HashLow: 0,
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_NODE_HASH))
                Expect(req.MessageBody.(MerkleNodeHash).NodeID).Should(Equal(server1.Buckets().Get("default").Node.MerkleTree().TranslateNode(nodeID, 3)))
                Expect(req.MessageBody.(MerkleNodeHash).HashHigh).Should(Equal(nodeHash.High()))
                Expect(req.MessageBody.(MerkleNodeHash).HashLow).Should(Equal(nodeHash.Low()))
                Expect(responderSyncSession.State()).Should(Equal(HASH_COMPARE))
            })
            
            It("HASH_COMPARE -> END SYNC_OBJECT_NEXT message with empty node", func() {
                responderSyncSession := NewResponderSyncSession(server1.Buckets().Get("default"))
                
                responderSyncSession.SetState(HASH_COMPARE)
                
                req := responderSyncSession.NextState(&SyncMessageWrapper{
                    SessionID: 123,
                    MessageType: SYNC_OBJECT_NEXT,
                    MessageBody: ObjectNext{ 
                        NodeID: 1,
                    },
                })
                
                Expect(req.SessionID).Should(Equal(uint(0)))
                Expect(req.MessageType).Should(Equal(SYNC_ABORT))
                Expect(responderSyncSession.State()).Should(Equal(END))
            })
        })
    })
})
