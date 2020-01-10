package builtin
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
    . "github.com/armPelionEdge/devicedb/storage"
    . "github.com/armPelionEdge/devicedb/resolver/strategies"
)

type LocalBucket struct {
    Store
}

func NewLocalBucket(nodeID string, storageDriver StorageDriver, merkleDepth uint8) (*LocalBucket, error) {
    localBucket := &LocalBucket{}

    err := localBucket.Initialize(nodeID, storageDriver, merkleDepth, &MultiValue{})

    if err != nil {
        return nil, err
    }

    return localBucket, nil
}

func (localBucket *LocalBucket) Name() string {
    return "local"
}

func (localBucket *LocalBucket) ShouldReplicateOutgoing(peerID string) bool {
    return false
}

func (localBucket *LocalBucket) ShouldReplicateIncoming(peerID string) bool {
    return false 
}

func (localBucket *LocalBucket) ShouldAcceptWrites(clientID string) bool {
    return true
}

func (localBucket *LocalBucket) ShouldAcceptReads(clientID string) bool {
    return true
}