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

func (cloudBucketProxyFactory *CloudBucketProxyFactory) CreateBucketProxy(nodeID uint64, siteID string, bucketName string) BucketProxy {
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
    return localBucketProxy.bucket.Name()
}

func (localBucketProxy *LocalBucketProxy) MerkleTree() MerkleTreeProxy {
    return &LocalMerkleTreeProxy{
        merkleTree: localBucketProxy.bucket.MerkleTree(),
    }
}

func (localBucketProxy *LocalBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    return localBucketProxy.bucket.GetSyncChildren(nodeID)
}

type RemoteBucketProxy struct {
    bucketName string
}

func (remoteBucketProxy *RemoteBucketProxy) Name() string {
    return remoteBucketProxy.bucketName
}

func (remoteBucketProxy *RemoteBucketProxy) MerkleTree() MerkleTreeProxy {
    return &RemoteMerkleTreeProxy{
    }
}

func (remoteBucketProxy *RemoteBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
}