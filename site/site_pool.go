package site

import (
    "sync"

)

type SitePool interface {
    // Called when client needs to access site. This does not guaruntee
    // exclusive access it merely ensures that the site pool does not
    // dispose of the underlying site
    Acquire(siteID string) Site
    // Called when client no longer needs access to a bucket.
    Release(siteID string)
}

// A relay only ever contains one site database
type RelayNodeSitePool struct {
    Site Site
}

func (relayNodeSitePool *RelayNodeSitePool) Acquire(siteID string) Site {
    return relayNodeSitePool.Site
}

func (relayNodeSitePool *RelayNodeSitePool) Release(siteID string) {
    // Do nothing. Relay bucket pool does no lifecycle management
    // for buckets.
}

type CloudNodeBucketPool struct {
    SiteFactory SiteFactory
    lock sync.Mutex
    sites map[string]Site
}

func (cloudNodeBucketPool *CloudNodeBucketPool) Acquire(siteID string) Site {
    cloudNodeBucketPool.lock.Lock()
    defer cloudNodeBucketPool.lock.Lock()

    if _, ok := cloudNodeBucketPool.sites[siteID]; !ok {
        cloudNodeBucketPool.sites[siteID] = cloudNodeBucketPool.SiteFactory.CreateSite(siteID)
    }

    return cloudNodeBucketPool.sites[siteID]
}

func (cloudNodeBucketPool *CloudNodeBucketPool) Release(siteID string) {
    // Do nothing for now.
}