package devicedb

// each namespace in the database has two main factors that differentiate it from other namespaces or buckets
// 1) Replication strategy
//        . some buckets are incoming only, we never send data from the bucket out to another node
//           - but are only incoming from a specific other node, i.e. some master node
//        . some buckets are both incoming and outgoing. Every database node shares with every other node
//        . some buckets can be neither, storing data local to that node only
// 2) Conflict resolution strategy
//        . whether or not to merge conflicting siblings
//        . the way in which conflicting sibilngs are merged into one sibling
type ReplicationStrategy interface {
    ShouldReplicateOutgoing(peerID string) bool
    ShouldReplicateIncoming(peerID string) bool
}

type Bucket struct {
    Node *Node
    ReplicationStrategy ReplicationStrategy
}

type BucketList struct {
    buckets map[string]Bucket
}

func NewBucketList() *BucketList {
    return &BucketList{ make(map[string]Bucket) }
}

func (bucketList *BucketList) AddBucket(name string, node *Node, replicationStrategy ReplicationStrategy) *BucketList {
    bucketList.buckets[name] = Bucket{ node, replicationStrategy }
    
    return bucketList
}

func (bucketList *BucketList) Outgoing(peerID string) []Bucket {
    buckets := make([]Bucket, 0, len(bucketList.buckets))
    
    for _, bucket := range bucketList.buckets {
        if bucket.ReplicationStrategy.ShouldReplicateOutgoing(peerID) {
            buckets = append(buckets, bucket)
        }
    }
    
    return buckets
}

func (bucketList *BucketList) Incoming(peerID string) []Bucket {
    buckets := make([]Bucket, 0, len(bucketList.buckets))
    
    for _, bucket := range bucketList.buckets {
        if bucket.ReplicationStrategy.ShouldReplicateIncoming(peerID) {
            buckets = append(buckets, bucket)
        }
    }
    
    return buckets
}

func (bucketList *BucketList) HasBucket(bucketName string) bool {
    _, ok := bucketList.buckets[bucketName]
    
    return ok
}

func (bucketList *BucketList) Get(bucketName string) Bucket {
    return bucketList.buckets[bucketName]
}