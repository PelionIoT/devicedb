package partition
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
    "sync"
)

type PartitionPool interface {
    Add(partition Partition)
    Remove(partitionNumber uint64)
    Get(partitionNumber uint64) Partition
}

type DefaultPartitionPool struct {
    lock sync.Mutex
    partitions map[uint64]Partition
}

func NewDefaultPartitionPool() *DefaultPartitionPool {
    return &DefaultPartitionPool{
        partitions: make(map[uint64]Partition, 0),
    }
}

func (partitionPool *DefaultPartitionPool) Add(partition Partition) {
    partitionPool.lock.Lock()
    defer partitionPool.lock.Unlock()

    partitionPool.partitions[partition.Partition()] = partition
}

func (partitionPool *DefaultPartitionPool) Remove(partitionNumber uint64) {
    partitionPool.lock.Lock()
    defer partitionPool.lock.Unlock()

    delete(partitionPool.partitions, partitionNumber)
}

func (partitionPool *DefaultPartitionPool) Get(partitionNumber uint64) Partition {
    partitionPool.lock.Lock()
    defer partitionPool.lock.Unlock()

    return partitionPool.partitions[partitionNumber]
}