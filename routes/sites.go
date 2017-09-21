package routes

import (
    "github.com/gorilla/mux"
    "net/http"
)

type SitesEndpoint struct {
    ClusterFacade ClusterFacade
}

func (sitesEndpoint *SitesEndpoint) Attach(router *mux.Router) {
    // Add a site
    router.HandleFunc("/sites/{siteID}", func(w http.ResponseWriter, r *http.Request) {
        //sitesEndpoint.ClusterFacade.AddSite(ctx, siteID)
    }).Methods("PUT")

    // Remove a site
    router.HandleFunc("/sites/{siteID}", func(w http.ResponseWriter, r *http.Request) {
        //sitesEndpoint.ClusterFacade.RemoveSite(ctx, siteID)
    }).Methods("DELETE")

    // Submit an update to a bucket
    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/batches", func(w http.ResponseWriter, r *http.Request) {
        //sitesEndpoint.ClusterFacade.Batch(siteID, bucket, update)
    }).Methods("POST")

    // Query keys in bucket
    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/keys", func(w http.ResponseWriter, r *http.Request) {
        //sitesEndpoint.ClusterFacade.Get(siteID, bucket, keys)
        //sitesEndpoint.ClusterFacade.GetMatches(siteID, bucket, keys)
        //Returns APIEntry
    }).Methods("GET")
}