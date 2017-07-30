package raft

import (
    "math"
    "time"
    "devicedb"
    
    "github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"
    "golang.org/x/net/context"
)

// Limit on the number of entries that can accumulate before a snapshot and compaction occurs
const LogCompactionSize = 1000

type RaftNodeConfig struct {
    ID uint64
    CreateClusterIfNotExist bool
    Storage *RaftStorage
    GetSnapshot func() ([]byte, error)
}

type RaftNode struct {
    config *RaftNodeConfig
    node raft.Node
    stop chan int
    messages chan raftpb.Message
    snapshots chan raftpb.Snapshot
    entries chan raftpb.Entry
    errors chan error
    currentRaftConfState raftpb.ConfState
}

func NewRaftNode(config *RaftNodeConfig) *RaftNode {
    raftNode := &RaftNode{
        config: config,
        node: nil,
        stop: make(chan int),
        messages: make(chan raftpb.Message),
        snapshots: make(chan raftpb.Snapshot),
        entries: make(chan raftpb.Entry),
        errors: make(chan error),
    }

    return raftNode
}

func (raftNode *RaftNode) AddNode(ctx context.Context, nodeID uint64) error {
    devicedb.Log.Infof("Node %d proposing addition of node %d to its cluster", raftNode.config.ID, nodeID)

    err := raftNode.node.ProposeConfChange(ctx, raftpb.ConfChange{
        ID: nodeID,
        Type: raftpb.ConfChangeAddNode,
        NodeID: nodeID,
        Context: []byte{ },
    })

    if err != nil {
        devicedb.Log.Errorf("Node %d was unable to propose addition of node %d to its cluster: %s", raftNode.config.ID, nodeID, err.Error())
    }

    return err
}

func (raftNode *RaftNode) RemoveNode(ctx context.Context, nodeID uint64) error {
    devicedb.Log.Infof("Node %d proposing removal of node %d from its cluster", raftNode.config.ID, nodeID)

    err := raftNode.node.ProposeConfChange(ctx, raftpb.ConfChange{
        ID: nodeID,
        Type: raftpb.ConfChangeRemoveNode,
        NodeID: nodeID,
        Context: []byte{ },
    })

    if err != nil {
        devicedb.Log.Errorf("Node %d was unable to propose removal of node %d from its cluster: %s", raftNode.config.ID, nodeID, err.Error())
    }

    return err
}

func (raftNode *RaftNode) Propose(ctx context.Context, proposition []byte) error {
    return raftNode.node.Propose(ctx, proposition)
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

// Errors returns a channel that receives any error that occurs
// In the main node processsing loop. An error receives on this
// channel indicates that the node has been stopped and the user
// should not attempt to call Stop() since it will block
func (raftNode *RaftNode) Errors() <-chan error {
    return raftNode.errors
}

func (raftNode *RaftNode) LastSnapshot() (raftpb.Snapshot, error) {
    return raftNode.config.Storage.Snapshot()
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

func (raftNode *RaftNode) Receive(ctx context.Context, msg raftpb.Message) error {
    return raftNode.node.Step(ctx, msg)
}

func (raftNode *RaftNode) run() {
    ticker := time.Tick(time.Second)

    defer func() {
        // makes sure cleanup happens when the loop exits
        raftNode.config.Storage.Close()
        raftNode.node.Stop()
        raftNode.currentRaftConfState = raftpb.ConfState{ }
    }()

    for {
        select {
        case <-ticker:
            raftNode.node.Tick()
        case rd := <-raftNode.node.Ready():
            if err := raftNode.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
                raftNode.errors <- err

                return
            }

            for _, msg := range rd.Messages {
                raftNode.messages <- msg
            }

            if !raft.IsEmptySnap(rd.Snapshot) {
                // snapshots received from other nodes. 
                // Used to allow this node to catch up
                raftNode.snapshots <- rd.Snapshot
            }

            for _, entry := range rd.CommittedEntries {
                raftNode.entries <- entry

                if entry.Type == raftpb.EntryConfChange {
                    if err := raftNode.applyConfigurationChange(entry); err != nil {
                        raftNode.errors <- err

                        return
                    }
                }
            }

            if err := raftNode.takeSnapshotIfEnoughEntries(); err != nil {
                raftNode.errors <- err
                
                return
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

    raftNode.currentRaftConfState = *raftNode.node.ApplyConfChange(confChange)

    return nil
}

func (raftNode *RaftNode) takeSnapshotIfEnoughEntries() error {
    lastSnapshot, err := raftNode.config.Storage.Snapshot()

    if err != nil {
        return err
    }

    lastIndex, err := raftNode.config.Storage.LastIndex()

    if err != nil {
        return err
    }

    if lastIndex < lastSnapshot.Metadata.Index {
        return nil
    }

    if lastIndex - lastSnapshot.Metadata.Index >= LogCompactionSize {
        // data is my config state snapshot
        data, err := raftNode.config.GetSnapshot()

        if err != nil {
            return err
        }

        devicedb.Log.Infof("Node %d compacting entries up to %d", raftNode.config.ID, lastIndex)
        _, err = raftNode.config.Storage.CreateSnapshot(lastIndex, &raftNode.currentRaftConfState, data)

        if err != nil {
            return err
        }
    }

    return nil
}

func (raftNode *RaftNode) Stop() {
    raftNode.stop <- 1
}