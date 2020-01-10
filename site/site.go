package site
//
 // Copyright (c) 2019 ARM Limited.
 //
 // SPDX-License-Identifier: MIT
 //
 // Permission is hereby granted, free of charge, to any person obtaining a copy
 // of this software and associated documentation files (the "Software"), to
 // deal in the Software without restriction, including without limitation the
 // rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 // sell copies of the Software, and to permit persons to whom the Software is
 // furnished to do so, subject to the following conditions:
 //
 // The above copyright notice and this permission notice shall be included in all
 // copies or substantial portions of the Software.
 //
 // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 // SOFTWARE.
 //


import (
    . "github.com/armPelionEdge/devicedb/bucket"
)

type Site interface {
    Buckets() *BucketList
    Iterator() SiteIterator
    ID() string
    LockWrites()
    UnlockWrites()
    LockReads()
    UnlockReads()
}

type RelaySiteReplica struct {
    bucketList *BucketList
    id string
}

func NewRelaySiteReplica(id string, buckets *BucketList) *RelaySiteReplica {
    return &RelaySiteReplica{
        id: id,
        bucketList: buckets,
    }
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

func (relaySiteReplica *RelaySiteReplica) LockWrites() {
}

func (relaySiteReplica *RelaySiteReplica) UnlockWrites() {
}

func (relaySiteReplica *RelaySiteReplica) LockReads() {
}

func (relaySiteReplica *RelaySiteReplica) UnlockReads() {
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

func (cloudSiteReplica *CloudSiteReplica) LockWrites() {
    for _, bucket := range cloudSiteReplica.bucketList.All() {
        bucket.LockWrites()
    }
}

func (cloudSiteReplica *CloudSiteReplica) UnlockWrites() {
    for _, bucket := range cloudSiteReplica.bucketList.All() {
        bucket.UnlockWrites()
    }
}

func (cloudSiteReplica *CloudSiteReplica) LockReads() {
    for _, bucket := range cloudSiteReplica.bucketList.All() {
        bucket.LockReads()
    }
}

func (cloudSiteReplica *CloudSiteReplica) UnlockReads() {
    for _, bucket := range cloudSiteReplica.bucketList.All() {
        bucket.UnlockReads()
    }
}
