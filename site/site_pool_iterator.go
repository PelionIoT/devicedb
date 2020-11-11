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
    "errors"

    . "github.com/PelionIoT/devicedb/data"
)

var EDecodeKey = errors.New("Unable to decode key in store")

type SitePoolIterator interface {
    Next() bool
    // The site that the current entry belongs to
    Site() string
    // The bucket that the current entry belongs to within its site
    Bucket() string
    // The key of the current entry
    Key() string
    // The value of the current entry
    Value() *SiblingSet
    // The checksum of the current entry
    Release()
    Error() error
}

type RelaySitePoolIterator struct {
}

func (relaySitePoolIterator *RelaySitePoolIterator) Next() bool {
    return true
}

func (relaySitePoolIterator *RelaySitePoolIterator) Site() string {
    return ""
}

func (relaySitePoolIterator *RelaySitePoolIterator) Bucket() string {
    return ""
}

func (relaySitePoolIterator *RelaySitePoolIterator) Key() string {
    return ""
}

func (relaySitePoolIterator *RelaySitePoolIterator) Value() *SiblingSet {
    return nil
}

func (relaySitePoolIterator *RelaySitePoolIterator) Release() {
}

func (relaySitePoolIterator *RelaySitePoolIterator) Error() error {
    return nil
}

type CloudSitePoolterator struct {
    currentSite string
    currentSiteIterator SiteIterator
    sites []string
    sitePool SitePool
    err error
}

func (cloudSitePoolIterator *CloudSitePoolterator) Next() bool {
    for {
        if cloudSitePoolIterator.currentSiteIterator == nil {
            if len(cloudSitePoolIterator.sites) == 0 {
                cloudSitePoolIterator.Release()
                
                return false
            }

            nextSiteID := cloudSitePoolIterator.sites[0]
            nextSite := cloudSitePoolIterator.sitePool.Acquire(nextSiteID)
            cloudSitePoolIterator.sites = cloudSitePoolIterator.sites[1:]

            if nextSite == nil {
                // This site must have been removed since iteration started
                // try again with the next site
                continue
            }

            cloudSitePoolIterator.currentSite = nextSite.ID()
            cloudSitePoolIterator.currentSiteIterator = nextSite.Iterator()
        }

        if !cloudSitePoolIterator.currentSiteIterator.Next() {
            cloudSitePoolIterator.currentSiteIterator.Release()
            cloudSitePoolIterator.sitePool.Release(cloudSitePoolIterator.currentSite)
            cloudSitePoolIterator.currentSite = ""

            if cloudSitePoolIterator.currentSiteIterator.Error() != nil {
                cloudSitePoolIterator.err = cloudSitePoolIterator.currentSiteIterator.Error()
                cloudSitePoolIterator.currentSiteIterator = nil
                cloudSitePoolIterator.Release()

                return false
            }

            cloudSitePoolIterator.currentSiteIterator = nil

            continue
        }

        return true
    }
}

func (cloudSitePoolIterator *CloudSitePoolterator) Site() string {
    return cloudSitePoolIterator.currentSite
}

func (cloudSitePoolIterator *CloudSitePoolterator) Bucket() string {
    return cloudSitePoolIterator.currentSiteIterator.Bucket()
}

func (cloudSitePoolIterator *CloudSitePoolterator) Key() string {
    return cloudSitePoolIterator.currentSiteIterator.Key()
}

func (cloudSitePoolIterator *CloudSitePoolterator) Value() *SiblingSet {
    return cloudSitePoolIterator.currentSiteIterator.Value()
}

func (cloudSitePoolIterator *CloudSitePoolterator) Release() {
    if cloudSitePoolIterator.currentSiteIterator != nil {
        cloudSitePoolIterator.sitePool.Release(cloudSitePoolIterator.currentSite)
        cloudSitePoolIterator.currentSiteIterator.Release()
    }

    cloudSitePoolIterator.currentSiteIterator = nil
    cloudSitePoolIterator.currentSite = ""

    for _, site := range cloudSitePoolIterator.sites {
        cloudSitePoolIterator.sitePool.Release(site)
    }

    cloudSitePoolIterator.sites = nil
    cloudSitePoolIterator.sitePool = nil
}

func (cloudSitePoolIterator *CloudSitePoolterator) Error() error {
    return cloudSitePoolIterator.err
}