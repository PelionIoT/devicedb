package raft

import (
    "math"
    "time"
    "devicedb"
    
    "github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"
    "golang.org/x/net/context"
)

type RaftNodeConfig struct {
    ID uint64
    CreateClusterIfNotExist bool
    Storage *RaftStorage
}

type RaftNode struct {
    config *RaftNodeConfig
    node raft.Node
    stop chan int
    messages chan raftpb.Message
    snapshots chan raftpb.Snapshot
    entries chan raftpb.Entry
}

func NewRaftNode(config *RaftNodeConfig) *RaftNode {
    raftNode := &RaftNode{
        config: config,
        node: nil,
        stop: make(chan int),
        messages: make(chan raftpb.Message),
        snapshots: make(chan raftpb.Snapshot),
        entries: make(chan raftpb.Entry),
    }

    return raftNode
}

func (raftNode *RaftNode) AddNode(nodeID uint64) error {
    devicedb.Log.Infof("Add node")
    return raftNode.node.ProposeConfChange(context.TODO(), raftpb.ConfChange{
        ID: nodeID,
        Type: raftpb.ConfChangeAddNode,
        NodeID: nodeID,
        Context: []byte{ },
    })
}

func (raftNode *RaftNode) RemoveNode(nodeID uint64) error {
    return raftNode.node.ProposeConfChange(context.TODO(), raftpb.ConfChange{
        ID: nodeID,
        Type: raftpb.ConfChangeRemoveNode,
        NodeID: nodeID,
        Context: []byte{ },
    })
}

func (raftNode *RaftNode) Propose(proposition []byte) error {
    return raftNode.node.Propose(context.TODO(), proposition)
}

func (raftNode *RaftNode) Messages() <-chan raftpb.Message {
    return raftNode.messages
}

func (raftNode *RaftNode) Snapshots() <-chan raftpb.Snapshot {
    return raftNode.snapshots
}

func (raftNode *RaftNode) Entries() <-chan raftpb.Entry {
    return raftNode.entries
}

func (raftNode *RaftNode) Start() error {
    if err := raftNode.config.Storage.Open(); err != nil {
        return err
    }

    config := &raft.Config{
        ID: raftNode.config.ID,
        ElectionTick: 10,
        HeartbeatTick: 1,
        Storage: raftNode.config.Storage,
        MaxSizePerMsg: math.MaxUint16,
        MaxInflightMsgs: 256,
    }

    if !raftNode.config.Storage.IsEmpty() {
        // indicates that this node has already been run before
        raftNode.node = raft.RestartNode(config)
    } else {
        // by default create a new cluster with one member (this node)
        peers := []raft.Peer{ raft.Peer{ ID: raftNode.config.ID } }

        if !raftNode.config.CreateClusterIfNotExist {
            // indicates that this node should join an existing cluster
            peers = nil
        }

        raftNode.node = raft.StartNode(config, peers)
    }

    go raftNode.run()

    return nil
}

func (raftNode *RaftNode) Receive(msg raftpb.Message) error {
    return raftNode.node.Step(context.TODO(), msg)
}

func (raftNode *RaftNode) run() {
    ticker := time.Tick(time.Second)

    for {
        select {
        case <-ticker:
            raftNode.node.Tick()
        case rd := <-raftNode.node.Ready():
            if err := raftNode.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
            }

            for _, msg := range rd.Messages {
                raftNode.messages <- msg
            }

            if !raft.IsEmptySnap(rd.Snapshot) {
                raftNode.snapshots <- rd.Snapshot
            }

            for _, entry := range rd.CommittedEntries {
                raftNode.entries <- entry

                if entry.Type == raftpb.EntryConfChange {
                    if err := raftNode.applyConfigurationChange(entry); err != nil {
                    }
                }
            }

            raftNode.node.Advance()
        case <-raftNode.stop:
            return
        }
    }
}

func (raftNode *RaftNode) saveToStorage(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
    if err := raftNode.config.Storage.Append(ents); err != nil {
        return err
    }

    if !raft.IsEmptyHardState(hs) {
        if err := raftNode.config.Storage.SetHardState(hs); err != nil {
            return err
        }
    }

    if !raft.IsEmptySnap(snap) {
        if err := raftNode.config.Storage.ApplySnapshot(snap); err != nil {
            return err
        }
    }

    return nil
}

func (raftNode *RaftNode) applyConfigurationChange(entry raftpb.Entry) error {
    var confChange raftpb.ConfChange

    if err := confChange.Unmarshal(entry.Data); err != nil {
        return err
    }

    raftNode.node.ApplyConfChange(confChange)

    return nil
}

func (raftNode *RaftNode) Stop() {
    raftNode.stop <- 1
    raftNode.config.Storage.Close()
    raftNode.node.Stop()
}