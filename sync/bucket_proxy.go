package sync

import (
    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/site"
)

type BucketProxyFactory interface {
    CreateBucketProxy(nodeID uint64, siteID string, bucket string) BucketProxy
}

type RelayBucketProxyFactory struct {
    // The site pool for this node
    SitePool SitePool
}

func (relayBucketProxyFactory *RelayBucketProxyFactory) CreateBucketProxy(nodeID uint64, siteID string, bucketName string) BucketProxy {
    return &LocalBucketProxy{
        bucket: relayBucketProxyFactory.SitePool.Acquire(siteID).Buckets().Get(bucketName),
    }
}

type CloudBucketProxyFactory struct {
    // The cluster controller for this node
    ClusterController *ClusterController
    // The site pool for this node
    SitePool SitePool
}

func (bucketProxyFactory *BucketProxyFactory) CreateBucketProxy(nodeID uint64, siteID string, bucketName string) BucketProxy {
    if bucketProxyFactory.ClusterController.LocalNodeID == nodeID {
        return &LocalBucketProxy{
            bucket: bucketProxyFactory.SitePool.Acquire(siteID).Buckets().Get(bucketName),
        }
    }

    return &RemoteBucketProxy{
    }
}

type BucketProxy interface {
    Name() string
    MerkleTree() MerkleTreeProxy
    GetSyncChildren(nodeID uint32) (SiblingSetIterator, error)
}

type LocalBucketProxy struct {
    bucket Bucket
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