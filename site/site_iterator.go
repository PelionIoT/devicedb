package site
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
    . "github.com/armPelionEdge/devicedb/data"
)

type SiteIterator interface {
    Next() bool
    // The site that the current entry belongs to
    Bucket() string
    // The key of the current entry
    Key() string
    // The value of the current entry
    Value() *SiblingSet
    // The checksum of the current entry
    Release()
    Error() error
}

type RelaySiteIterator struct {
}

func (relaySiteIterator *RelaySiteIterator) Next() bool {
    return false
}

func (relaySiteIterator *RelaySiteIterator) Bucket() string {
    return ""
}

func (relaySiteIterator *RelaySiteIterator) Key() string {
    return ""
}

func (relaySiteIterator *RelaySiteIterator) Value() *SiblingSet {
    return nil
}

func (relaySiteIterator *RelaySiteIterator) Release() {
}

func (relaySiteIterator *RelaySiteIterator) Error() error {
    return nil
}

type CloudSiteIterator struct {
    buckets []Bucket
    currentIterator SiblingSetIterator
    currentBucket string
    currentKey string
    currentValue *SiblingSet
    err error
}

func (cloudSiteIterator *CloudSiteIterator) Next() bool {
    if cloudSiteIterator.currentIterator == nil {
        if len(cloudSiteIterator.buckets) == 0 {
            return false
        }

        nextBucket := cloudSiteIterator.buckets[0]
        cloudSiteIterator.currentBucket = nextBucket.Name()

        iter, err := nextBucket.GetAll()

        if err != nil {
            cloudSiteIterator.err = err
            cloudSiteIterator.Release()

            return false
        }

        cloudSiteIterator.currentIterator = iter
        cloudSiteIterator.buckets = cloudSiteIterator.buckets[1:]
    }

    if !cloudSiteIterator.currentIterator.Next() {
        if cloudSiteIterator.currentIterator.Error() != nil {
            cloudSiteIterator.err = cloudSiteIterator.currentIterator.Error()
            cloudSiteIterator.Release()

            return false
        }

        cloudSiteIterator.currentIterator = nil

        return cloudSiteIterator.Next()
    }

    cloudSiteIterator.currentKey = string(cloudSiteIterator.currentIterator.Key())
    cloudSiteIterator.currentValue = cloudSiteIterator.currentIterator.Value()

    return true
}

func (cloudSiteIterator *CloudSiteIterator) Bucket() string {
    if cloudSiteIterator == nil {
        return ""
    }

    return cloudSiteIterator.currentBucket
}

func (cloudSiteIterator *CloudSiteIterator) Key() string {
    if cloudSiteIterator == nil {
        return ""
    }

    return cloudSiteIterator.currentKey
}

func (cloudSiteIterator *CloudSiteIterator) Value() *SiblingSet {
    if cloudSiteIterator == nil {
        return nil
    }

    return cloudSiteIterator.currentValue
}

func (cloudSiteIterator *CloudSiteIterator) Release() {
    if cloudSiteIterator.currentIterator != nil {
        cloudSiteIterator.currentIterator.Release()
    }

    cloudSiteIterator.currentIterator = nil
    cloudSiteIterator.buckets = nil
    cloudSiteIterator.currentBucket = ""
    cloudSiteIterator.currentKey = ""
    cloudSiteIterator.currentValue = nil
}

func (cloudSiteIterator *CloudSiteIterator) Error() error {
    return cloudSiteIterator.err
}
