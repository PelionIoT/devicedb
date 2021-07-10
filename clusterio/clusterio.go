package clusterio
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
    "context"

    . "github.com/PelionIoT/devicedb/bucket"
    . "github.com/PelionIoT/devicedb/data"
    . "github.com/PelionIoT/devicedb/routes"
)

type ClusterIOAgent interface {
    Merge(ctx context.Context, siteID string, bucket string, patch map[string]*SiblingSet) (replicas int, nApplied int, err error)
    Batch(ctx context.Context, siteID string, bucket string, updateBatch *UpdateBatch) (replicas int, nApplied int, err error)
    Get(ctx context.Context, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error)
    GetMatches(ctx context.Context, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error)
    RelayStatus(ctx context.Context, siteID string, relayID string) (RelayStatus, error)
    CancelAll()
}

type PartitionResolver interface {
    Partition(partitioningKey string) uint64
    ReplicaNodes(partition uint64) []uint64
}

type NodeClient interface {
    Merge(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error
    Batch(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) (map[string]*SiblingSet, error)
    Get(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error)
    GetMatches(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error)
    RelayStatus(ctx context.Context, nodeID uint64, siteID string, relayID string) (RelayStatus, error)
    LocalNodeID() uint64
}

type NodeReadMerger interface {
    // Add to the pool of replicas for this key
    InsertKeyReplica(nodeID uint64, key string, siblingSet *SiblingSet)
    // Get the merged set for this key
    Get(key string) *SiblingSet
    // Obtain a patch that needs to be merged into the specified node to bring it up to date
    // for any keys for which there are updates that it has not received
    Patch(nodeID uint64) map[string]*SiblingSet
    // Get a set of nodes involved in the read merger
    Nodes() map[uint64]bool
}

type NodeReadRepairer interface {
    BeginRepair(partition uint64, siteID string, bucket string, readMerger NodeReadMerger)
    StopRepairs()
}