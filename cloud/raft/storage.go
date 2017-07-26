package raft

import (
    "sync"

    "devicedb"
    "github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"
)

type RaftStorage struct {
    storageDriver devicedb.StorageDriver
    lock sync.Mutex
}

// START raft.Storage interface methods
func (raftStorage *RaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
}

func (raftStorage *RaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
}

func (raftStorage *RaftStorage) Term(i uint64) (uint64, error) {
}

func (raftStorage *RaftStorage) LastIndex() (uint64, error) {
}

func (raftStorage *RaftStorage) FirstIndex() (uint64, error) {
}

func (raftStorage *RaftStorage) Snapshot() (raftpb.Snapshot, error) {
}
// END raft.Storage interface methods

func (raftStorage *RaftStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
}

func (raftStorage *RaftStorage) Compact(compactIndex uint64) error {
}

func (raftStorage *RaftStorage) Append(entries []raftpb.Entry) error {
}