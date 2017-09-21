package routes

import (
    "github.com/gorilla/mux"
    "net/http"
)


// Submitting an update
// [client] -> [coordinator] -> { [replica] [replica] [replica] }

type PartitionsEndpoint struct {
}

func (partitionsEndpoint *PartitionsEndpoint) Attach(router *mux.Router) {
    // Submit an update to a bucket
    router.HandleFunc("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches", func(w http.ResponseWriter, r *http.Request) {
        // Batch(siteID, bucket, update)
    }).Methods("POST")

    // Query keys in bucket
    router.HandleFunc("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/keys", func(w http.ResponseWriter, r *http.Request) {
        // Get(siteID, bucket, update)
        // GetMatches(siteID, bucket, update)
        // Returns InternalEntry
    }).Methods("GET")
}