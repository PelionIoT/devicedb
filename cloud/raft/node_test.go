package raft_test

import (
    "devicedb"
    "time"
    . "devicedb/cloud/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("Node", func() {
    It("should work", func() {
        node1 := NewRaftNode(&RaftNodeConfig{
            ID: 0x1,
            CreateClusterIfNotExist: true,
            Storage: NewRaftStorage(devicedb.NewLevelDBStorageDriver("/tmp/testraftstore-" + randomString(), nil)),
        })

        node2 := NewRaftNode(&RaftNodeConfig{
            ID: 0x2,
            CreateClusterIfNotExist: false,
            Storage: NewRaftStorage(devicedb.NewLevelDBStorageDriver("/tmp/testraftstore-" + randomString(), nil)),
        })

        node3 := NewRaftNode(&RaftNodeConfig{
            ID: 0x3,
            CreateClusterIfNotExist: false,
            Storage: NewRaftStorage(devicedb.NewLevelDBStorageDriver("/tmp/testraftstore-" + randomString(), nil)),
        })

        nodeMap := map[uint64]*RaftNode{
            1: node1,
            2: node2,
            3: node3,
        }

        var run = func(id uint64, node *RaftNode) {
            for {
                select {
                case msg := <-node.Messages():
                    //devicedb.Log.Infof("Message received by %d: %v", id, msg)
                    nodeMap[msg.To].Receive(msg)
                case <-node.Snapshots():
                case entry := <-node.Entries():
                    devicedb.Log.Infof("New entry at node %d: %v", id, entry)
                }
            }
        }

        go run(1, node1)
        go run(2, node2)
        go run(3, node3)

        Expect(node1.Start()).Should(BeNil())
        Expect(node2.Start()).Should(BeNil())
        Expect(node3.Start()).Should(BeNil())
        <-time.After(time.Second * 1)
        Expect(node1.AddNode(2)).Should(BeNil())
        <-time.After(time.Second * 1)
        Expect(node1.AddNode(3)).Should(BeNil())
        devicedb.Log.Infof("After adds")
        <-time.After(time.Second * 5)

        go node1.Propose([]byte(randomString()))
        go node2.Propose([]byte(randomString()))
        go node3.Propose([]byte(randomString()))
        go node1.Propose([]byte(randomString()))
        go node2.Propose([]byte(randomString()))
        go node3.Propose([]byte(randomString()))

        <-time.After(time.Second * 10)
    })
})
