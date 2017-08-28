package site

import (
    . "devicedb/bucket"
)

type Site interface {
    Buckets() *BucketList
}

type RelaySiteReplica struct {
    bucketList *BucketList
}

func (relaySiteReplica *RelaySiteReplica) Buckets() *BucketList {
    if relaySiteReplica == nil {
        return NewBucketList()
    }

    return relaySiteReplica.bucketList
}

type CloudSiteReplica struct {
    bucketList *BucketList
}

func (cloudSiteReplica *CloudSiteReplica) Buckets() *BucketList {
    if cloudSiteReplica == nil {
        return NewBucketList()
    }

    return cloudSiteReplica.bucketList
}
