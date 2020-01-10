// How to gossip sites and relays??
// set<site>
// map<relay, site>
// Using gossip protocol for this association could result in inconsistencies for the client:
//
// Asks node 1 to add site so node 1 adds site to its local map. node 1 responds with a success code
// Before node 1 can gossip to other nodes the client asks node 2 something about site 1 and since node 2 has not yet heard about it it returns an error
// 
// 
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


package gossiper

type ClusterConfigurationListener interface {
    // Invoked after the node has joined the cluster
    OnJoined(func())
    // Invoked after the node has left or been forcibly removed from the cluster
    OnLeft(func())
    OnGainPartitionReplica(func(partition, replica uint64))
    OnLostPartitionReplica(func(partition, replica uint64))
    OnGainedPartitionReplicaOwnership(func(partition, replica uint64))
    OnLostPartitionReplicaOwnership(func(partition, replica uint64))
    OnSiteAdded(func(siteID string))
    OnSiteRemoved(func(siteID string))
    OnRelayAdded(func(relayID string))
    OnRelayRemoved(func(relayID string))
    OnRelayMoved(func(relayID string, siteID string))
}

type ClusterConfiguration interface {
    AddNode()
    RemoveNode()
    ReplaceNode()
}

type ClusterState interface {
    // Get the NodeState associated with this node
    Get(nodeID uint64) NodeState
    // Add a new node state for this node
    Add(nodeID uint64)
    Digest() map[uint64]NodeStateVersion
}

type NodeState interface {
    Tick()
    Heartbeat() NodeStateVersion
    Get(key string) NodeStateEntry
    Put(key string, value string)
    Version() NodeStateVersion
    LatestEntries(minVersion NodeStateVersion) []NodeStateEntry
}

type NodeStateEntry interface {
    Key() string
    Value() string
    Version() NodeStateVersion
}

type NodeStateVersion interface {
    Version() uint64
}