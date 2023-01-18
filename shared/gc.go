package shared

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
	"time"

	. "github.com/PelionIoT/devicedb/bucket"
	. "github.com/PelionIoT/devicedb/logging"
)

type GarbageCollector struct {
	buckets    *BucketList
	gcInterval time.Duration
	gcPurgeAge uint64
	done       chan bool
}

func NewGarbageCollector(buckets *BucketList, gcInterval uint64, gcPurgeAge uint64) *GarbageCollector {
	return &GarbageCollector{
		buckets:    buckets,
		gcInterval: time.Millisecond * time.Duration(gcInterval),
		gcPurgeAge: gcPurgeAge,
		done:       make(chan bool),
	}
}

func (garbageCollector *GarbageCollector) Start() {
	go func() {
		for {
			select {
			case <-garbageCollector.done:
				garbageCollector.done = make(chan bool)
				return
			case <-time.After(garbageCollector.gcInterval):
				for _, bucket := range garbageCollector.buckets.All() {
					Log.Infof("Performing garbage collection sweep on %s bucket", bucket.Name())
					bucket.GarbageCollect(garbageCollector.gcPurgeAge)
				}
			}
		}
	}()
}

func (garbageCollector *GarbageCollector) Stop() {
	close(garbageCollector.done)
}
