package routes

import (
    "encoding/json"
    "github.com/gorilla/mux"
    "io"
    "net/http"

	. "devicedb/error"
	. "devicedb/logging"
)

type SnapshotEndpoint struct {
    ClusterFacade ClusterFacade
}

func (snapshotEndpoint *SnapshotEndpoint) Attach(router *mux.Router) {
    router.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
        snapshot, err := snapshotEndpoint.ClusterFacade.ClusterSnapshot(r.Context())

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
	
	router.HandleFunc("/snapshot/{snapshotId}.tar", func(w http.ResponseWriter, r *http.Request) {
		var snapshotId string = mux.Vars(r)["snapshotId"]

		err := snapshotEndpoint.ClusterFacade.CheckLocalSnapshotStatus(snapshotId)

		if err == ESnapshotInProgress {
            Log.Warningf("GET /snapshot/{snapshotId}: %v", err)
			
			w.Header().Set("Content-Type", "application/json; charset=utf8")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, string(ESnapshotInProgress.JSON()) + "\n")

			return
		} else if err == ESnapshotOpenFailed {
            Log.Warningf("GET /snapshot/{snapshotId}: %v", err)
			
			w.Header().Set("Content-Type", "application/json; charset=utf8")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, string(ESnapshotOpenFailed.JSON()) + "\n")

			return
		} else if err == ESnapshotReadFailed {
            Log.Warningf("GET /snapshot/{snapshotId}: %v", err)
			
			w.Header().Set("Content-Type", "application/json; charset=utf8")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, string(ESnapshotReadFailed.JSON()) + "\n")

			return
		} else if err != nil {
			Log.Warningf("GET /snapshot/{snapshotId}: %v", err)
			
			w.Header().Set("Content-Type", "application/json; charset=utf8")
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, string(EStorage.JSON()) + "\n")

			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		err = snapshotEndpoint.ClusterFacade.WriteLocalSnapshot(snapshotId, w)

		if err != nil {
			Log.Errorf("GET /snapshot/{snapshotId}: %v", err)
		}
	}).Methods("GET")
}