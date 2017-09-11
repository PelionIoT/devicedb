package routes

import (
    "github.com/gorilla/mux"
)

type ClusterEndpoint struct {
}

func (clusterEndpoint *ClusterEndpoint) Attach(router *mux.router) {
    router.HandleFunc("/cluster/nodes", func(w http.ResponseWriter, r *http.Request) {
        // Add a node to the cluster
        body, err := ioutil.ReadAll(r.Body)

        if err != nil {
            Log.Warningf("POST /cluster/nodes: %v", err)
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(EReadBody.JSON()) + "\n")
            
            return
        }

        var nodeConfig NodeConfig

        if err := json.Unmarshal(body, &nodeConfig); err != nil {
            Log.Warningf("POST /cluster/nodes: Unable to parse node config body")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, string(ENodeConfigBody.JSON()) + "\n")
            
            return
        }

        if err := server.configController.AddNode(r.Context(), nodeConfig); err != nil {
            Log.Warningf("POST /cluster/nodes: Unable to add node to cluster: %v", err.Error())

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)

            if err == ECancelConfChange {
                io.WriteString(w, string(EDuplicateNodeID.JSON()) + "\n")
            } else {
                io.WriteString(w, string(EProposalError.JSON()) + "\n")
            }
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("POST")

    router.HandleFunc("/cluster/nodes/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
        // Remove, replace, or deccommission a node
        query := r.URL.Query()
        _, wasForwarded := query["forwarded"]
        _, replace := query["replace"]
        _, decommission := query["decommission"]

        if replace && decommission {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Both the replace and decommission query parameters are set. This is not allowed")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        nodeID, err := strconv.ParseUint(mux.Vars(r)["nodeID"], 10, 64)

        if err != nil {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Invalid node ID")
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")
            
            return
        }

        if nodeID == 0 {
            nodeID = server.clusterController.LocalNodeID
        }

        if decommission {
            if nodeID == server.clusterController.LocalNodeID {
                if err := server.raftStore.SetDecommissioningFlag(); err != nil {
                    Log.Warningf("DELETE /cluster/nodes/{nodeID}: Encountered an error while setting the decommissioning flag: %v", err.Error())
                    
                    w.Header().Set("Content-Type", "application/json; charset=utf8")
                    w.WriteHeader(http.StatusInternalServerError)
                    io.WriteString(w, "\n")
                    
                    return
                }

                go server.leaveCluster()

                return
            }

            if wasForwarded {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Received a forwarded decommission request but we're not the correct node")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusForbidden)
                io.WriteString(w, "\n")
                
                return
            } 
            
            // forward the request to another node
            peerAddress := server.raftTransportHub.PeerAddress(nodeID)

            if peerAddress == nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Unable to forward decommission request since this node doesn't know how to contact the decommissioned node")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadGateway)
                io.WriteString(w, "\n")
                
                return
            }

            err := server.interClusterClient.RemoveNode(r.Context(), *peerAddress, nodeID, 0, true, true)

            if err != nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Error forwarding decommission request: %v", err.Error())
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadGateway)
                io.WriteString(w, "\n")
                
                return
            }

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusOK)
            io.WriteString(w, "\n")

            return
        }

        var replacementNodeID uint64

        if replace {
            replacementNodeID, err = strconv.ParseUint(query["replace"][0], 10, 64)

            if err != nil {
                Log.Warningf("DELETE /cluster/nodes/{nodeID}: Invalid replacement node ID")
                
                w.Header().Set("Content-Type", "application/json; charset=utf8")
                w.WriteHeader(http.StatusBadRequest)
                io.WriteString(w, "\n")
                
                return
            }
        }

        if replacementNodeID != 0 {
            err = server.configController.ReplaceNode(r.Context(), nodeID, replacementNodeID)
        } else {
            err = server.configController.RemoveNode(r.Context(), nodeID)
        }

        if err != nil {
            Log.Warningf("DELETE /cluster/nodes/{nodeID}: Unable to remove node from the cluster: %v", err.Error())
            
            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")
            
            return
        }

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, "\n")
    }).Methods("DELETE")
}