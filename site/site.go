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
    return relaySiteReplica.bucketList
}

type CloudSiteReplica struct {
    bucketList *BucketList
}

func (cloudSiteReplica *CloudSiteReplica) Buckets() *BucketList {
    return cloudSiteReplica.bucketList
}
