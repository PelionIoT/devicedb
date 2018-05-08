package routes

import (
    "encoding/json"
    "github.com/gorilla/mux"
    "io"
    "net/http"

    . "devicedb/logging"
)

type SnapshotEndpoint struct {
    ClusterFacade ClusterFacade
}

func (snapshotEndpoint *SnapshotEndpoint) Attach(router *mux.Router) {
    router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
        snapshot, err := snapshotEndpoint.ClusterFacade.LocalSnapshot()

        if err != nil {
            Log.Warningf("POST /snapshot: %v", err)

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, err.Error())
            
            return
        }

        encodedSnapshot, err := json.Marshal(snapshot)

        if err != nil {
            Log.Warningf("POST /snapshot: %v", err)

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, string(encodedSnapshot) + "\n")
    }).Methods("POST")
}