package raft

import (
    "github.com/coreos/etcd/raft"
)

type RaftNodeConfig struct {
    ID uint64
    CreateClusterIfNotExist bool
    Storage *RaftStorage
}

type RaftNode struct {
    config *RaftNodeConfig
    node *raft.Node
}

func NewRaftNode(config *RaftNodeConfig) *RaftNode {
    raftNode := &RaftNode{
        config: config,
        node: nil,
    }

    return raftNode
}

func (raftNode *RaftNode) Start() {
    /*config := &raft.Config{
        ID: raftNode.config.ID,
        ElectionTick: 10,
        HeartbeatTick: 1,
        Storage: raftNode.config.Storage,
        MaxSizePerMsg: math.MaxUint16,
        MaxInflightMsgs: 256,
    }*/

    // it may the first node in a new cluster
    /*raftNode.Node = &raft.StartNode(config, []raft.Peer{ raft.Peer{ ID: raftNode.ID } })
    // it may be a node that will need to join an existing cluster
    raftNode.Node = &raft.StartNode(config, nil)
    // this node may already be part of a cluster
    raftNode.Node = &raft.RestartNode(config)*/
}

func (raftNode *RaftNode) Stop() {
    //raftNode.Node.Stop()
}