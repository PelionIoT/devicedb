package sync

import (
    "context"
    "errors"
    "math/rand"

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
    // Return a set of buckets for which updates can be
    // pushed from the given node to this node/cluster
    IncomingBuckets(peerID string) map[string]bool
    // Return a set of buckets for which updates can be
    // pushed from this node/cluster to the given node
    OutgoingBuckets(peerID string) map[string]bool
    // Create a bucket proxy to the bucket specified in the site
    // that the peer belongs to
    CreateBucketProxy(peerID string, bucket string) (BucketProxy, error)
}

type RelayBucketProxyFactory struct {
    // The site pool for this node
    SitePool SitePool
}

func (relayBucketProxyFactory *RelayBucketProxyFactory) CreateBucketProxy(peerID string, bucketName string) (BucketProxy, error) {
    site := relayBucketProxyFactory.SitePool.Acquire("")

    if site.Buckets().Get(bucketName) == nil {
        return nil, ENoLocalBucket
    }

    return &RelayBucketProxy{
        Bucket: site.Buckets().Get(bucketName),
        SitePool: relayBucketProxyFactory.SitePool,
        SiteID: "",
    }, nil
}

func (relayBucketProxyFactory *RelayBucketProxyFactory) IncomingBuckets(peerID string) map[string]bool {
    var buckets map[string]bool = make(map[string]bool)

    site := relayBucketProxyFactory.SitePool.Acquire("")

    for _, bucket := range site.Buckets().Incoming(peerID) {
        buckets[bucket.Name()] = true
    }

    return buckets
}

func (relayBucketProxyFactory *RelayBucketProxyFactory) OutgoingBuckets(peerID string) map[string]bool {
    var buckets map[string]bool = make(map[string]bool)

    site := relayBucketProxyFactory.SitePool.Acquire("")

    for _, bucket := range site.Buckets().Outgoing(peerID) {
        buckets[bucket.Name()] = true
    }

    return buckets
}

type CloudBucketProxyFactory struct {
    // An intra-cluster client
    Client Client
    // The cluster controller for this node
    ClusterController *ClusterController
    // The site pool for this node
    SitePool SitePool
}

func (cloudBucketProxyFactory *CloudBucketProxyFactory) CreateBucketProxy(peerID string, bucketName string) (BucketProxy, error) {
    siteID := cloudBucketProxyFactory.ClusterController.RelaySite(peerID)
    partitionNumber := cloudBucketProxyFactory.ClusterController.Partition(siteID)
    nodeIDs := cloudBucketProxyFactory.ClusterController.PartitionOwners(partitionNumber)

    if len(nodeIDs) == 0 {
        return nil, errors.New("No node owns this partition")
    }

    // Choose a node at random from the nodes that own this site database
    nodeID := nodeIDs[int(rand.Uint32() % uint32(len(nodeIDs)))]

    if cloudBucketProxyFactory.ClusterController.LocalNodeID == nodeID {
        site := cloudBucketProxyFactory.SitePool.Acquire(siteID)

        if site == nil || site.Buckets().Get(bucketName) == nil {
            return nil, ENoLocalBucket
        }

        localBucket := &RelayBucketProxy{
            Bucket: site.Buckets().Get(bucketName),
            SitePool: cloudBucketProxyFactory.SitePool,
            SiteID: siteID,
        }

        return localBucket, nil
    }

    return &CloudResponderBucketProxy{
        Client: cloudBucketProxyFactory.Client,
        PeerAddress: cloudBucketProxyFactory.ClusterController.ClusterMemberAddress(nodeID),
        SiteID: siteID,
        BucketName: bucketName,
    }, nil
}

func (cloudBucketProxyFactory *CloudBucketProxyFactory) IncomingBuckets(peerID string) map[string]bool {
    return map[string]bool{ "default": true, "lww": true }
}

func (cloudBucketProxyFactory *CloudBucketProxyFactory) OutgoingBuckets(peerID string) map[string]bool {
    return map[string]bool{ "default": true, "lww": true, "cloud": true }
}

type BucketProxy interface {
    Name() string
    MerkleTree() MerkleTreeProxy
    GetSyncChildren(nodeID uint32) (SiblingSetIterator, error)
    Merge(mergedKeys map[string]*SiblingSet) error
    Forget(keys [][]byte) error
    Close()
}

type RelayBucketProxy struct {
    Bucket Bucket
    SiteID string
    SitePool SitePool
}

func (relayBucketProxy *RelayBucketProxy) Name() string {
    return relayBucketProxy.Bucket.Name()
}

func (relayBucketProxy *RelayBucketProxy) MerkleTree() MerkleTreeProxy {
    return &DirectMerkleTreeProxy{
        merkleTree: relayBucketProxy.Bucket.MerkleTree(),
    }
}

func (relayBucketProxy *RelayBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    return relayBucketProxy.Bucket.GetSyncChildren(nodeID)
}

func (relayBucketProxy *RelayBucketProxy) Close() {
    relayBucketProxy.SitePool.Release(relayBucketProxy.SiteID)
}

func (relayBucketProxy *RelayBucketProxy) Merge(mergedKeys map[string]*SiblingSet) error {
    return relayBucketProxy.Bucket.Merge(mergedKeys)
}

func (relayBucketProxy *RelayBucketProxy) Forget(keys [][]byte) error {
    return relayBucketProxy.Bucket.Forget(keys)
}

type CloudResponderBucketProxy struct {
    Client Client
    PeerAddress PeerAddress
    SiteID string
    BucketName string
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) Name() string {
    return cloudResponderBucketProxy.BucketName
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) MerkleTree() MerkleTreeProxy {
    merkleTreeStats, err := cloudResponderBucketProxy.Client.MerkleTreeStats(context.TODO(), cloudResponderBucketProxy.PeerAddress, cloudResponderBucketProxy.SiteID, cloudResponderBucketProxy.BucketName)

    if err != nil {
        return &CloudResponderMerkleTreeProxy{
            err: err,
        }
    }

    dummyMerkleTree, err := NewDummyMerkleTree(merkleTreeStats.Depth)

    if err != nil {
        return &CloudResponderMerkleTreeProxy{
            err: err,
        }
    }

    return &CloudResponderMerkleTreeProxy{
        err: nil,
        client: cloudResponderBucketProxy.Client,
        peerAddress: cloudResponderBucketProxy.PeerAddress,
        siteID: cloudResponderBucketProxy.SiteID,
        bucketName: cloudResponderBucketProxy.BucketName,
        merkleTree: dummyMerkleTree,
    }
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    merkleKeys, err := cloudResponderBucketProxy.Client.MerkleTreeNodeKeys(context.TODO(), cloudResponderBucketProxy.PeerAddress, cloudResponderBucketProxy.SiteID, cloudResponderBucketProxy.BucketName, nodeID)

    if err != nil {
        return nil, err
    }

    return &CloudResponderMerkleNodeIterator{
        MerkleKeys: merkleKeys,
        CurrentIndex: -1,
    }, nil
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) Merge(mergedKeys map[string]*SiblingSet) error {
    return nil
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) Forget(keys [][]byte) error {
    return nil
}

func (cloudResponderBucketProxy *CloudResponderBucketProxy) Close() {
}

type CloudResponderMerkleNodeIterator struct {
    MerkleKeys rest.MerkleKeys
    CurrentIndex int
}

func (iter *CloudResponderMerkleNodeIterator) Next() bool {
    if iter.CurrentIndex >= len(iter.MerkleKeys.Keys) - 1 {
        iter.CurrentIndex = len(iter.MerkleKeys.Keys)

        return false
    }

    iter.CurrentIndex++

    return true
}

func (iter *CloudResponderMerkleNodeIterator) Prefix() []byte {
    return nil
}

func (iter *CloudResponderMerkleNodeIterator) Key() []byte {
    if iter.CurrentIndex < 0 || len(iter.MerkleKeys.Keys) == 0 || iter.CurrentIndex >= len(iter.MerkleKeys.Keys) {
        return nil
    }

    return []byte(iter.MerkleKeys.Keys[iter.CurrentIndex].Key)
}

func (iter *CloudResponderMerkleNodeIterator) Value() *SiblingSet {
    if iter.CurrentIndex < 0 || len(iter.MerkleKeys.Keys) == 0 || iter.CurrentIndex >= len(iter.MerkleKeys.Keys) {
        return nil
    }

    return iter.MerkleKeys.Keys[iter.CurrentIndex].Value
}

func (iter *CloudResponderMerkleNodeIterator) Release() {
}

func (iter *CloudResponderMerkleNodeIterator) Error() error {
    return nil
}

type CloudInitiatorBucketProxy struct {
    Client Client
    ClusterController ClusterController
    Bucket Bucket
    SiteID string
    SitePool SitePool
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) Name() string {
    return cloudInitiatorBucketProxy.Bucket.Name()
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) MerkleTree() MerkleTreeProxy {
    return &DirectMerkleTreeProxy{ merkleTree: cloudInitiatorBucketProxy.Bucket.MerkleTree() }
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    return nil, nil
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) Merge(mergedKeys map[string]*SiblingSet) error {
    return nil
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) Forget(keys [][]byte) error {
    return nil
}

func (cloudInitiatorBucketProxy *CloudInitiatorBucketProxy) Close() {
    cloudInitiatorBucketProxy.SitePool.Release(cloudInitiatorBucketProxy.SiteID)
}