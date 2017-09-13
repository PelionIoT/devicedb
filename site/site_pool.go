package site

import (
    "sync"
)

type SitePool interface {
    // Called when client needs to access site. This does not guaruntee
    // exclusive access it merely ensures that the site pool does not
    // dispose of the underlying site
    Acquire(siteID string) Site
    // Called when client no longer needs access to a site
    Release(siteID string)
    // Call when a site should be added to the pool
    Add(siteID string)
    // Called when a site should be removed from the pool
    Remove(siteID string)
    // Iterate over all sites that exist in the site pool
    Iterator() SitePoolIterator
}

// A relay only ever contains one site database
type RelayNodeSitePool struct {
    Site Site
}

func (relayNodeSitePool *RelayNodeSitePool) Acquire(siteID string) Site {
    return relayNodeSitePool.Site
}

func (relayNodeSitePool *RelayNodeSitePool) Release(siteID string) {
}

func (relayNodeSitePool *RelayNodeSitePool) Add(siteID string) {
}

func (relayNodeSitePool *RelayNodeSitePool) Remove(siteID string) {
}

func (relayNodeSitePool *RelayNodeSitePool) Iterator() SitePoolIterator {
    return &RelaySitePoolIterator{ }
}

type CloudNodeSitePool struct {
    SiteFactory SiteFactory
    lock sync.Mutex
    sites map[string]Site
}

func (cloudNodeSitePool *CloudNodeSitePool) Acquire(siteID string) Site {
    cloudNodeSitePool.lock.Lock()
    defer cloudNodeSitePool.lock.Unlock()

    site, ok := cloudNodeSitePool.sites[siteID]

    if !ok {
        return nil
    }

    if site == nil {
        cloudNodeSitePool.sites[siteID] = cloudNodeSitePool.SiteFactory.CreateSite(siteID)
    }

    return cloudNodeSitePool.sites[siteID]
}

func (cloudNodeSitePool *CloudNodeSitePool) Release(siteID string) {
}

func (cloudNodeSitePool *CloudNodeSitePool) Add(siteID string) {
    cloudNodeSitePool.lock.Lock()
    defer cloudNodeSitePool.lock.Unlock()

    if cloudNodeSitePool.sites == nil {
        cloudNodeSitePool.sites = make(map[string]Site)
    }

    if _, ok := cloudNodeSitePool.sites[siteID]; !ok {
        cloudNodeSitePool.sites[siteID] = nil
    }
}

func (cloudNodeSitePool *CloudNodeSitePool) Remove(siteID string) {
    cloudNodeSitePool.lock.Lock()
    defer cloudNodeSitePool.lock.Unlock()

    delete(cloudNodeSitePool.sites, siteID)
}

func (cloudNodeSitePool *CloudNodeSitePool) Iterator() SitePoolIterator {
    cloudNodeSitePool.lock.Lock()
    defer cloudNodeSitePool.lock.Unlock()

    // take a snapshot of the currently available sites
    sites := make([]string, 0, len(cloudNodeSitePool.sites))

    for siteID, _ := range cloudNodeSitePool.sites {
        sites = append(sites, siteID)
    }

    return &CloudSitePoolterator{ sites: sites, sitePool: cloudNodeSitePool }
}