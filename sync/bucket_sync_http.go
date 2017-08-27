package sync

import (
    "time"
    "strings"
    "github.com/gorilla/mux"

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

type BucketSyncHTTP {
}

func (hub *TransportHub) Attach(router *mux.Router) {
    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle", func(w http.ResponseWriter, r *http.Request) {
        // Get merkle tree stats including its depth
    }).Methods("GET")

    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle/nodes/{nodeID}/keys", func(w http.ResponseWriter, r *http.Request) {
        // Get a key range under a merkle node
    }).Methods("GET")

    router.HandleFunc("/sites/{siteID}/buckets/{bucket}/merkle/nodes/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
        // Get the hash of a node
    }).Methods("GET")
}