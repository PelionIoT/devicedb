package routes

import (
    "encoding/json"
    "github.com/gorilla/mux"
    "io"
    "net/http"
    "strconv"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/error"
    . "devicedb/logging"
)

type PartitionsEndpoint struct {
    ClusterFacade ClusterFacade
}

func (partitionsEndpoint *PartitionsEndpoint) Attach(router *mux.Router) {
    // Submit an update to a bucket
    router.HandleFunc("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches", func(w http.ResponseWriter, r *http.Request) {
        var updateBatch UpdateBatch
        var err error

        err = updateBatch.FromJSON(r.Body)

        if err != nil {
            Log.Warningf("POST /partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches: Unable to parse request body: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        partitionID, err := strconv.ParseUint(mux.Vars(r)["partitionID"], 10, 64)

        if err != nil {
            Log.Warningf("POST /partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches: Unable to parse partition ID as uint64: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        var siteID string = mux.Vars(r)["siteID"]
        var bucket string = mux.Vars(r)["bucketID"]

        err = partitionsEndpoint.ClusterFacade.LocalBatch(partitionID, siteID, bucket, &updateBatch)

        if err == ENoSuchPartition || err == ENoSuchSite || err == EBucketDoesNotExist {
            Log.Warningf("POST /partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, "\n")
            
            return
        }

        if err != nil && err != ENoQuorum {
            Log.Warningf("POST /partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/batches: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        var batchResult BatchResult
        batchResult.NApplied = 1
        
        if err == ENoQuorum {
            batchResult.NApplied = 0
        }

        encodedBatchResult, _ := json.Marshal(batchResult)

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, string(encodedBatchResult) + "\n")
    }).Methods("POST")

    // Query keys in bucket
    router.HandleFunc("/partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/keys", func(w http.ResponseWriter, r *http.Request) {
        query := r.URL.Query()
        keys := query["key"]
        prefixes := query["prefix"]

        if len(keys) != 0 && len(prefixes) != 0 {
            Log.Warningf("GET /partitions/{partitionID}/sites/{siteID}/buckets/{bucketID}/keys: Client specified both prefixes and keys in the same request")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        if len(keys) == 0 && len(prefixes) == 0 {
            var entries []InternalEntry = []InternalEntry{ }
            encodedEntries, _ := json.Marshal(entries)

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusOK)
            io.WriteString(w, string(encodedEntries) + "\n")
            
            return
        }
    }).Methods("GET")
}