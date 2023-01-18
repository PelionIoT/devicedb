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
	. "github.com/PelionIoT/devicedb/bucket"
	. "github.com/PelionIoT/devicedb/bucket/builtin"
	. "github.com/PelionIoT/devicedb/merkle"
	. "github.com/PelionIoT/devicedb/storage"
)

var keyStorePrefix = []byte{0}

const (
	defaultNodePrefix = iota
	cloudNodePrefix   = iota
	lwwNodePrefix     = iota
	localNodePrefix   = iota
	historianPrefix   = iota
	alertsLogPrefix   = iota
)

type SiteFactory interface {
	CreateSite(siteID string) Site
}

type RelaySiteFactory struct {
	MerkleDepth   uint8
	StorageDriver StorageDriver
	RelayID       string
}

func (relaySiteFactory *RelaySiteFactory) CreateSite(siteID string) Site {
	bucketList := NewBucketList()

	defaultBucket, _ := NewDefaultBucket(relaySiteFactory.RelayID, NewPrefixedStorageDriver([]byte{defaultNodePrefix}, relaySiteFactory.StorageDriver), relaySiteFactory.MerkleDepth)
	cloudBucket, _ := NewCloudBucket(relaySiteFactory.RelayID, NewPrefixedStorageDriver([]byte{cloudNodePrefix}, relaySiteFactory.StorageDriver), relaySiteFactory.MerkleDepth, RelayMode)
	lwwBucket, _ := NewLWWBucket(relaySiteFactory.RelayID, NewPrefixedStorageDriver([]byte{lwwNodePrefix}, relaySiteFactory.StorageDriver), relaySiteFactory.MerkleDepth)
	localBucket, _ := NewLocalBucket(relaySiteFactory.RelayID, NewPrefixedStorageDriver([]byte{localNodePrefix}, relaySiteFactory.StorageDriver), MerkleMinDepth)

	bucketList.AddBucket(defaultBucket)
	bucketList.AddBucket(lwwBucket)
	bucketList.AddBucket(cloudBucket)
	bucketList.AddBucket(localBucket)

	return &RelaySiteReplica{
		bucketList: bucketList,
		id:         siteID,
	}
}

type CloudSiteFactory struct {
	NodeID        string
	MerkleDepth   uint8
	StorageDriver StorageDriver
}

func (cloudSiteFactory *CloudSiteFactory) siteBucketStorageDriver(siteID string, bucketPrefix []byte) StorageDriver {
	return NewPrefixedStorageDriver(cloudSiteFactory.siteBucketPrefix(siteID, bucketPrefix), cloudSiteFactory.StorageDriver)
}

func (cloudSiteFactory *CloudSiteFactory) siteBucketPrefix(siteID string, bucketPrefix []byte) []byte {
	prefix := make([]byte, 0, len(keyStorePrefix)+len([]byte(siteID))+len([]byte("."))+len(bucketPrefix)+len([]byte(".")))

	prefix = append(prefix, keyStorePrefix...)
	prefix = append(prefix, []byte(siteID)...)
	prefix = append(prefix, []byte(".")...)
	prefix = append(prefix, bucketPrefix...)
	prefix = append(prefix, []byte(".")...)

	return prefix
}

func (cloudSiteFactory *CloudSiteFactory) CreateSite(siteID string) Site {
	bucketList := NewBucketList()

	defaultBucket, _ := NewDefaultBucket(cloudSiteFactory.NodeID, cloudSiteFactory.siteBucketStorageDriver(siteID, []byte{defaultNodePrefix}), cloudSiteFactory.MerkleDepth)
	cloudBucket, _ := NewCloudBucket(cloudSiteFactory.NodeID, cloudSiteFactory.siteBucketStorageDriver(siteID, []byte{cloudNodePrefix}), cloudSiteFactory.MerkleDepth, CloudMode)
	lwwBucket, _ := NewLWWBucket(cloudSiteFactory.NodeID, cloudSiteFactory.siteBucketStorageDriver(siteID, []byte{lwwNodePrefix}), cloudSiteFactory.MerkleDepth)
	localBucket, _ := NewLocalBucket(cloudSiteFactory.NodeID, cloudSiteFactory.siteBucketStorageDriver(siteID, []byte{localNodePrefix}), MerkleMinDepth)

	bucketList.AddBucket(defaultBucket)
	bucketList.AddBucket(lwwBucket)
	bucketList.AddBucket(cloudBucket)
	bucketList.AddBucket(localBucket)

	return &CloudSiteReplica{
		bucketList: bucketList,
		id:         siteID,
	}
}
