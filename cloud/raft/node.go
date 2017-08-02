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
    lastCommittedIndex uint64
    onMessagesCB func([]raftpb.Message) error
    onSnapshotCB func(raftpb.Snapshot) error
    onEntryCB func(raftpb.Entry) error
    onErrorCB func(error) error
    currentRaftConfState raftpb.ConfState
}

func NewRaftNode(config *RaftNodeConfig) *RaftNode {
    raftNode := &RaftNode{
        config: config,
        node: nil,
        stop: make(chan int),
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

    lastSnapshot, _ := raftNode.LastSnapshot()

    if !raft.IsEmptySnap(lastSnapshot) {
        // call onSnapshot callback to give initial state to system config
        raftNode.onSnapshotCB(lastSnapshot)
        raftNode.lastCommittedIndex = lastSnapshot.Metadata.Index
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
            // Saves raft state to persistent storage first. If the process dies or fails after this point
            // This ensures that there is a checkpoint to resume from on restart
            if err := raftNode.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
                raftNode.onErrorCB(err)

                return
            }

            // Messages must be sent after entries and hard state are saved to stable storage
            if len(rd.Messages) != 0 {
                raftNode.onMessagesCB(rd.Messages)
            }

            if !raft.IsEmptySnap(rd.Snapshot) {
                // snapshots received from other nodes. 
                // Used to allow this node to catch up
                raftNode.onSnapshotCB(rd.Snapshot)
                raftNode.lastCommittedIndex = rd.Snapshot.Metadata.Index
            }

            for _, entry := range rd.CommittedEntries {
                raftNode.onEntryCB(entry)

                raftNode.lastCommittedIndex = entry.Index

                if entry.Type == raftpb.EntryConfChange {
                    if err := raftNode.applyConfigurationChange(entry); err != nil {
                        raftNode.onErrorCB(err)

                        return
                    }
                }
            }

            // Snapshot current state and perform a compaction of entries
            // if the number of entries exceeds a certain theshold
            if err := raftNode.takeSnapshotIfEnoughEntries(); err != nil {
                raftNode.onErrorCB(err)
                
                return
            }

            raftNode.node.Advance()
        case <-raftNode.stop:
            return
        }
    }
}

func (raftNode *RaftNode) saveToStorage(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
    // Ensures that all updates get applied atomically to persistent storage: HardState, Entries, Snapshot.
    // If any part of the update fails then no change is applied. This is important so that the persistent state
    // remains consistent.
    if err := raftNode.config.Storage.ApplyAll(hs, ents, snap); err != nil {
        return err
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

    if raftNode.lastCommittedIndex < lastSnapshot.Metadata.Index {
        return nil
    }

    if raftNode.lastCommittedIndex - lastSnapshot.Metadata.Index >= LogCompactionSize {
        // data is my config state snapshot
        data, err := raftNode.config.GetSnapshot()

        if err != nil {
            return err
        }

        devicedb.Log.Infof("Node %d compacting entries up to %d", raftNode.config.ID, raftNode.lastCommittedIndex)
        _, err = raftNode.config.Storage.CreateSnapshot(raftNode.lastCommittedIndex, &raftNode.currentRaftConfState, data)

        if err != nil {
            return err
        }
    }

    return nil
}

func (raftNode *RaftNode) Stop() {
    raftNode.stop <- 1
}

func (raftNode *RaftNode) OnMessages(cb func([]raftpb.Message) error) {
    raftNode.onMessagesCB = cb
}

func (raftNode *RaftNode) OnSnapshot(cb func(raftpb.Snapshot) error) {
    raftNode.onSnapshotCB = cb
}

func (raftNode *RaftNode) OnCommittedEntry(cb func(raftpb.Entry) error) {
    raftNode.onEntryCB = cb
}

func (raftNode *RaftNode) OnError(cb func(error) error) {
    raftNode.onErrorCB = cb
}

func (raftNode *RaftNode) ReportUnreachable(id uint64) {
}

func (raftNode *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
}