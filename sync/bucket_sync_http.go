package sync

import (
    "time"
    "strings"
    "github.com/gorilla/mux"

    . "devicedb/cluster"
    . "devicedb/site"
    . "devicedb/logging"

    "fmt"
    "errors"
    "net/http"
    "bytes"
    "io"
    "io/ioutil"
    "context"
    "sync"
)

type BucketSyncHTTP struct {
    ClusterController *ClusterController
    SitePool SitePool
}

func (bucketSync *BucketSyncHTTP) Attach(router *mux.Router) {
    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle", func(w http.ResponseWriter, r *http.Request) {
        // Get merkle tree stats including its depth
        // 1) Ensure site exists. Return 404 if it doesnt
        // 2) Get site from site pool. Ensure bucket exists. Return 404 if it doesnt
        // 3) Get merkle depth
    }).Methods("GET")

    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle/nodes/{nodeID}/keys", func(w http.ResponseWriter, r *http.Request) {
        // Get a key range under a merkle node
    }).Methods("GET")

    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle/nodes/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
        // Get the hash of a node
    }).Methods("GET")
}