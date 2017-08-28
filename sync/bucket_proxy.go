package sync

import (
    "context"
    "errors"

    . "devicedb/bucket"
    . "devicedb/client"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/site"
    . "devicedb/raft"
    rest "devicedb/rest"
    . "devicedb/merkle"
)

var ENoLocalBucket = errors.New("No such bucket exists locally")

type BucketProxyFactory interface {
    CreateBucketProxy(nodeID uint64, siteID string, bucket string) (BucketProxy, error)
}

type RelayBucketProxyFactory struct {
    // The site pool for this node
    SitePool SitePool
}

func (relayBucketProxyFactory *RelayBucketProxyFactory) CreateBucketProxy(nodeID uint64, siteID string, bucketName string) (BucketProxy, error) {
    site := relayBucketProxyFactory.SitePool.Acquire(siteID)

    if site.Buckets().Get(bucketName) == nil {
        return nil, ENoLocalBucket
    }

    return &LocalBucketProxy{
        Bucket: site.Buckets().Get(bucketName),
        SitePool: relayBucketProxyFactory.SitePool,
        SiteID: siteID,
    }, nil
}

type CloudBucketProxyFactory struct {
    // An intra-cluster client
    Client Client
    // The cluster controller for this node
    ClusterController *ClusterController
    // The site pool for this node
    SitePool SitePool
}

func (cloudBucketProxyFactory *CloudBucketProxyFactory) CreateBucketProxy(nodeID uint64, siteID string, bucketName string) (BucketProxy, error) {
    if cloudBucketProxyFactory.ClusterController.LocalNodeID == nodeID {
        site := cloudBucketProxyFactory.SitePool.Acquire(siteID)

        if site == nil || site.Buckets().Get(bucketName) == nil {
            return nil, ENoLocalBucket
        }

        localBucket := &LocalBucketProxy{
            Bucket: site.Buckets().Get(bucketName),
            SitePool: cloudBucketProxyFactory.SitePool,
            SiteID: siteID,
        }

        return localBucket, nil
    }

    return &RemoteBucketProxy{
        Client: cloudBucketProxyFactory.Client,
        PeerAddress: cloudBucketProxyFactory.ClusterController.ClusterMemberAddress(nodeID),
        SiteID: siteID,
        BucketName: bucketName,
    }, nil
}

type BucketProxy interface {
    Name() string
    MerkleTree() MerkleTreeProxy
    GetSyncChildren(nodeID uint32) (SiblingSetIterator, error)
    Close()
}

type LocalBucketProxy struct {
    Bucket Bucket
    SiteID string
    SitePool SitePool
}

func (localBucketProxy *LocalBucketProxy) Name() string {
    return localBucketProxy.Bucket.Name()
}

func (localBucketProxy *LocalBucketProxy) MerkleTree() MerkleTreeProxy {
    return &LocalMerkleTreeProxy{
        merkleTree: localBucketProxy.Bucket.MerkleTree(),
    }
}

func (localBucketProxy *LocalBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    return localBucketProxy.Bucket.GetSyncChildren(nodeID)
}

func (localBucketProxy *LocalBucketProxy) Close() {
    localBucketProxy.SitePool.Release(localBucketProxy.SiteID)
}

type RemoteBucketProxy struct {
    Client Client
    PeerAddress PeerAddress
    SiteID string
    BucketName string
}

func (remoteBucketProxy *RemoteBucketProxy) Name() string {
    return remoteBucketProxy.BucketName
}

func (remoteBucketProxy *RemoteBucketProxy) MerkleTree() MerkleTreeProxy {
    merkleTreeStats, err := remoteBucketProxy.Client.MerkleTreeStats(context.TODO(), remoteBucketProxy.PeerAddress, remoteBucketProxy.SiteID, remoteBucketProxy.BucketName)

    if err != nil {
        return &RemoteMerkleTreeProxy{
            err: err,
        }
    }

    dummyMerkleTree, err := NewDummyMerkleTree(merkleTreeStats.Depth)

    if err != nil {
        return &RemoteMerkleTreeProxy{
            err: err,
        }
    }

    return &RemoteMerkleTreeProxy{
        err: err,
        client: remoteBucketProxy.Client,
        peerAddress: remoteBucketProxy.PeerAddress,
        siteID: remoteBucketProxy.SiteID,
        bucketName: remoteBucketProxy.BucketName,
        merkleTree: dummyMerkleTree,
    }
}

func (remoteBucketProxy *RemoteBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    merkleKeys, err := remoteBucketProxy.Client.MerkleTreeNodeKeys(context.TODO(), remoteBucketProxy.PeerAddress, remoteBucketProxy.SiteID, remoteBucketProxy.BucketName, nodeID)

    if err != nil {
        return nil, err
    }

    return &RemoteMerkleNodeSiblingSetIterator{
        merkleKeys: merkleKeys,
        currentIndex: -1,
    }, nil
}

func (remoteBucketProxy *RemoteBucketProxy) Close() {
}

type RemoteMerkleNodeSiblingSetIterator struct {
    merkleKeys rest.MerkleKeys
    currentIndex int
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Next() bool {
    iter.currentIndex++

    if iter.currentIndex >= len(iter.merkleKeys.Keys) {
        return false
    }

    return true
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Prefix() []byte {
    return nil
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Key() []byte {
    return []byte(iter.merkleKeys.Keys[iter.currentIndex].Key)
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Value() *SiblingSet {
    return iter.merkleKeys.Keys[iter.currentIndex].Value
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Release() {
}

func (iter *RemoteMerkleNodeSiblingSetIterator) Error() error {
    return nil
}
