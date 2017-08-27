package site

import (
    . "devicedb/bucket"
    . "devicedb/storage"
)

type Site interface {
    Buckets() *BucketList
}

func New(nodeID string, storageDriver StorageDriver, merkleDepth uint32) Site {
}

type RelaySiteReplica struct {
    bucketList *BucketList
}

func (relaySiteReplica *RelaySiteReplica) Buckets() *BucketList {
    return relaySiteReplica.bucketList
}

type CloudSiteReplica struct {
    bucketList *BucketList
}

func (cloudSiteReplica *CloudSiteReplica) Buckets() *BucketList {
    return cloudSiteReplica.bucketList
}
