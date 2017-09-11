package raft

import (
    "github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"
)

type RaftMemoryStorage struct {
    raft.MemoryStorage
    isEmpty bool
    isDecomissioning bool
    nodeID uint64
}

func NewRaftMemoryStorage() *RaftMemoryStorage {
    return &RaftMemoryStorage{
        isEmpty: true,
    }
}

func (raftStorage *RaftMemoryStorage) Open() error {
    return nil
}

func (raftStorage *RaftMemoryStorage) Close() error {
    return nil
}

func (raftStorage *RaftMemoryStorage) IsEmpty() bool {
    return raftStorage.isEmpty
}

func (raftStorage *RaftMemoryStorage) SetDecommissioningFlag() error {
    raftStorage.isDecomissioning = true

    return nil
}

func (raftStorage *RaftMemoryStorage) IsDecommissioning() (bool, error) {
    return raftStorage.isDecomissioning, nil
}

func (raftStorage *RaftMemoryStorage) SetNodeID(id uint64) error {
    raftStorage.nodeID = id

    return nil
}

func (raftStorage *RaftMemoryStorage) NodeID() (uint64, error) {
    return raftStorage.nodeID, nil
}

func (raftStorage *RaftMemoryStorage) ApplyAll(hs raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
    raftStorage.Append(ents)

    // update hard state if set
    if !raft.IsEmptyHardState(hs) {
        raftStorage.SetHardState(hs)
    }

    // apply snapshot
    if !raft.IsEmptySnap(snap) {
        raftStorage.ApplySnapshot(snap)
    }

    return nil
}