package routes

import (
    "github.com/gorilla/mux"
    "net/http"
)

type RelaysEndpoint struct {
}

func (relaysEndpoint *RelaysEndpoint) Attach(router *mux.Router) {
    // Add relay or move it to a site
    router.HandleFunc("/relays/{relayID}", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("PUT")

    // Remove a relay and disassociate it from a site
    router.HandleFunc("/relays/{relayID}", func(w http.ResponseWriter, r *http.Request) {
    }).Methods("DELETE")
}