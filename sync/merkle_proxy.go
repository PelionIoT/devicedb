package sync

import (
    "context"

    . "devicedb/client"
    . "devicedb/data"
    . "devicedb/merkle"
    . "devicedb/raft"
)

type MerkleTreeProxy interface {
    RootNode() uint32
    Depth() uint8
    NodeLimit() uint32
    NodeHash(nodeID uint32) Hash
    TranslateNode(nodeID uint32, depth uint8) uint32
    Error() error
}

type LocalMerkleTreeProxy struct {
    merkleTree *MerkleTree
}

func (localMerkleProxy *LocalMerkleTreeProxy) MerkleTree() *MerkleTree {
    return localMerkleProxy.merkleTree
}

func (localMerkleProxy *LocalMerkleTreeProxy) RootNode() uint32 {
    return localMerkleProxy.merkleTree.RootNode()
}

func (localMerkleProxy *LocalMerkleTreeProxy) Depth() uint8 {
    return localMerkleProxy.merkleTree.Depth()
}

func (localMerkleProxy *LocalMerkleTreeProxy) NodeLimit() uint32 {
    return localMerkleProxy.merkleTree.NodeLimit()
}

func (localMerkleProxy *LocalMerkleTreeProxy) NodeHash(nodeID uint32) Hash {
    return localMerkleProxy.merkleTree.NodeHash(nodeID)
}

func (localMerkleProxy *LocalMerkleTreeProxy) TranslateNode(nodeID uint32, depth uint8) uint32 {
    return localMerkleProxy.merkleTree.TranslateNode(nodeID, depth)
}

func (localMerkleProxy *LocalMerkleTreeProxy) Error() error {
    return nil
}

type RemoteMerkleTreeProxy struct {
    err error
    client Client
    peerAddress PeerAddress
    siteID string
    bucketName string
    merkleTree *MerkleTree
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) RootNode() uint32 {
    if remoteMerkleProxy.err != nil {
        return 0
    }

    return remoteMerkleProxy.merkleTree.RootNode()
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) Depth() uint8 {
    if remoteMerkleProxy.err != nil {
        return 0
    }

    return remoteMerkleProxy.merkleTree.Depth()
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) NodeLimit() uint32 {
    if remoteMerkleProxy.err != nil {
        return 0
    }

    return remoteMerkleProxy.merkleTree.NodeLimit()
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) NodeHash(nodeID uint32) Hash {
    if remoteMerkleProxy.err != nil {
        return Hash{}
    }

    merkleNode, err := remoteMerkleProxy.client.MerkleTreeNode(context.TODO(), remoteMerkleProxy.peerAddress, remoteMerkleProxy.siteID, remoteMerkleProxy.bucketName, nodeID)

    if err != nil {
        remoteMerkleProxy.err = err

        return Hash{}
    }

    return merkleNode.Hash
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) TranslateNode(nodeID uint32, depth uint8) uint32 {
    if remoteMerkleProxy.err != nil {
        return 0
    }

    return remoteMerkleProxy.merkleTree.TranslateNode(nodeID, depth)
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) Error() error {
    return remoteMerkleProxy.err
}