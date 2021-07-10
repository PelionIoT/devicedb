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
    . "github.com/PelionIoT/devicedb/bucket"
    . "github.com/PelionIoT/devicedb/storage"
    . "github.com/PelionIoT/devicedb/resolver/strategies"
)

type LWWBucket struct {
    Store
}

func NewLWWBucket(nodeID string, storageDriver StorageDriver, merkleDepth uint8) (*LWWBucket, error) {
    lwwBucket := &LWWBucket{}

    err := lwwBucket.Initialize(nodeID, storageDriver, merkleDepth, &LastWriterWins{})

    if err != nil {
        return nil, err
    }

    return lwwBucket, nil
}

func (lwwBucket *LWWBucket) Name() string {
    return "lww"
}

func (lwwBucket *LWWBucket) ShouldReplicateOutgoing(peerID string) bool {
    return true 
}

func (lwwBucket *LWWBucket) ShouldReplicateIncoming(peerID string) bool {
    return true
}

func (lwwBucket *LWWBucket) ShouldAcceptWrites(clientID string) bool {
    return true
}

func (lwwBucket *LWWBucket) ShouldAcceptReads(clientID string) bool {
    return true
}