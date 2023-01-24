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
	. "github.com/PelionIoT/devicedb/site"
)

type Partition interface {
	Partition() uint64
	Sites() SitePool
	Iterator() PartitionIterator
	LockWrites()
	UnlockWrites()
	LockReads()
	UnlockReads()
}

type DefaultPartition struct {
	partition uint64
	sitePool  SitePool
}

func NewDefaultPartition(partition uint64, sitePool SitePool) *DefaultPartition {
	return &DefaultPartition{
		partition: partition,
		sitePool:  sitePool,
	}
}

func (partition *DefaultPartition) Partition() uint64 {
	return partition.partition
}

func (partition *DefaultPartition) Sites() SitePool {
	return partition.sitePool
}

func (partition *DefaultPartition) Iterator() PartitionIterator {
	return partition.sitePool.Iterator()
}

func (partition *DefaultPartition) LockWrites() {
}

func (partition *DefaultPartition) UnlockWrites() {
}

func (partition *DefaultPartition) LockReads() {
}

func (partition *DefaultPartition) UnlockReads() {
}
