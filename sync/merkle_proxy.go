package sync

import (
    . "devicedb/data"
    . "devicedb/merkle"
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
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) RootNode() uint32 {
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) Depth() uint8 {
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) NodeLimit() uint32 {
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) NodeHash(nodeID uint32) Hash {
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) TranslateNode(nodeID uint32, depth uint8) uint32 {
}

func (remoteMerkleProxy *RemoteMerkleTreeProxy) Error() error {
}