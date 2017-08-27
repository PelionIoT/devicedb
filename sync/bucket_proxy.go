package sync

import (
    . "devicedb/data"
)

type BucketProxy interface {
    Name() string
    MerkleTree() MerkleTreeProxy
    GetSyncChildren(nodeID uint32) (SiblingSetIterator, error)
}

type LocalBucketProxy struct {
}

func (localBucketProxy *LocalBucketProxy) Name() string {
}

func (localBucketProxy *LocalBucketProxy) MerkleTree() MerkleTreeProxy {
}

func (localBucketProxy *LocalBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
}

type RemoteBucketProxy struct {
}

func (remoteBucketProxy *RemoteBucketProxy) Name() string {
}

func (remoteBucketProxy *RemoteBucketProxy) MerkleTree() MerkleTreeProxy {
}

func (remoteBucketProxy *RemoteBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
}
