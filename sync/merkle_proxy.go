package sync
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

    . "github.com/armPelionEdge/devicedb/client"
    . "github.com/armPelionEdge/devicedb/data"
    . "github.com/armPelionEdge/devicedb/merkle"
    . "github.com/armPelionEdge/devicedb/raft"
)

type MerkleTreeProxy interface {
    RootNode() uint32
    Depth() uint8
    NodeLimit() uint32
    Level(nodeID uint32) uint8
    LeftChild(nodeID uint32) uint32
    RightChild(nodeID uint32) uint32
    NodeHash(nodeID uint32) Hash
    TranslateNode(nodeID uint32, depth uint8) uint32
    Error() error
}

type DirectMerkleTreeProxy struct {
    merkleTree *MerkleTree
}

func (directMerkleProxy *DirectMerkleTreeProxy) MerkleTree() *MerkleTree {
    return directMerkleProxy.merkleTree
}

func (directMerkleProxy *DirectMerkleTreeProxy) RootNode() uint32 {
    return directMerkleProxy.merkleTree.RootNode()
}

func (directMerkleProxy *DirectMerkleTreeProxy) Depth() uint8 {
    return directMerkleProxy.merkleTree.Depth()
}

func (directMerkleProxy *DirectMerkleTreeProxy) NodeLimit() uint32 {
    return directMerkleProxy.merkleTree.NodeLimit()
}

func (directMerkleProxy *DirectMerkleTreeProxy) Level(nodeID uint32) uint8 {
    return directMerkleProxy.merkleTree.Level(nodeID)
}

func (directMerkleProxy *DirectMerkleTreeProxy) LeftChild(nodeID uint32) uint32 {
    return directMerkleProxy.merkleTree.LeftChild(nodeID)
}

func (directMerkleProxy *DirectMerkleTreeProxy) RightChild(nodeID uint32) uint32 {
    return directMerkleProxy.merkleTree.RightChild(nodeID)
}

func (directMerkleProxy *DirectMerkleTreeProxy) NodeHash(nodeID uint32) Hash {
    return directMerkleProxy.merkleTree.NodeHash(nodeID)
}

func (directMerkleProxy *DirectMerkleTreeProxy) TranslateNode(nodeID uint32, depth uint8) uint32 {
    return directMerkleProxy.merkleTree.TranslateNode(nodeID, depth)
}

func (directMerkleProxy *DirectMerkleTreeProxy) Error() error {
    return nil
}

type CloudResponderMerkleTreeProxy struct {
    err error
    client Client
    peerAddress PeerAddress
    siteID string
    bucketName string
    merkleTree *MerkleTree
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) RootNode() uint32 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.RootNode()
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) Depth() uint8 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.Depth()
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) NodeLimit() uint32 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.NodeLimit()
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) Level(nodeID uint32) uint8 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.Level(nodeID)
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) LeftChild(nodeID uint32) uint32 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.LeftChild(nodeID)
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) RightChild(nodeID uint32) uint32 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.RightChild(nodeID)
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) NodeHash(nodeID uint32) Hash {
    if cloudResponderMerkleProxy.err != nil {
        return Hash{}
    }

    merkleNode, err := cloudResponderMerkleProxy.client.MerkleTreeNode(context.TODO(), cloudResponderMerkleProxy.peerAddress, cloudResponderMerkleProxy.siteID, cloudResponderMerkleProxy.bucketName, nodeID)

    if err != nil {
        cloudResponderMerkleProxy.err = err

        return Hash{}
    }

    return merkleNode.Hash
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) TranslateNode(nodeID uint32, depth uint8) uint32 {
    if cloudResponderMerkleProxy.err != nil {
        return 0
    }

    return cloudResponderMerkleProxy.merkleTree.TranslateNode(nodeID, depth)
}

func (cloudResponderMerkleProxy *CloudResponderMerkleTreeProxy) Error() error {
    return cloudResponderMerkleProxy.err
}