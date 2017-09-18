package routes

import (
    "github.com/gorilla/mux"
    "net/http"
)

type SitesEndpoint struct {
}

func (sitesEndpoint *SitesEndpoint) Attach(router *mux.Router) {
    // Add a site
    router.HandleFunc("/sites/{siteID}", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("PUT")

    // Remove a site
    router.HandleFunc("/sites/{siteID}", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("DELETE")

    // Submit an update to a bucket
    router.HandleFunc("/sites/{siteID}/buckets/{bucketID}/batches", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("POST")

    // Query keys in bucket
    router.HandleFunc("/sites/{siteID}/buckets/{bucketID}/keys", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("GET")
}