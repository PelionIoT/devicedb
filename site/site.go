package site

import (
    . "devicedb/bucket"
)

type Site interface {
    Buckets() *BucketList
    Iterator() SiteIterator
    ID() string
}

type RelaySiteReplica struct {
    bucketList *BucketList
    id string
}

func (relaySiteReplica *RelaySiteReplica) Buckets() *BucketList {
    if relaySiteReplica == nil {
        return NewBucketList()
    }

    return relaySiteReplica.bucketList
}

func (relaySiteReplica *RelaySiteReplica) ID() string {
    return relaySiteReplica.id
}

func (relaySiteReplica *RelaySiteReplica) Iterator() SiteIterator {
    return &RelaySiteIterator{ }
}

type CloudSiteReplica struct {
    bucketList *BucketList
    id string
}

func (cloudSiteReplica *CloudSiteReplica) Buckets() *BucketList {
    if cloudSiteReplica == nil {
        return NewBucketList()
    }

    return cloudSiteReplica.bucketList
}

func (cloudSiteReplica *CloudSiteReplica) ID() string {
    return cloudSiteReplica.id
}

func (cloudSiteReplica *CloudSiteReplica) Iterator() SiteIterator {
    return &CloudSiteIterator{ buckets: cloudSiteReplica.bucketList.All() }
}