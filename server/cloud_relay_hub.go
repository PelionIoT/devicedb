package server

import (
    "github.com/gorilla/websocket"

    . "devicedb/cluster"
)

type RelayHub struct {
    ClusterController ClusterController
}

func (relayHub *RelayHub) AcceptRelayConnection(relayID string, connection *websocket.Conn) {
    // proxy or accept relay connection
    // Get siteID of relay from ClusterController
    // Get partition number of that site from replication strategy
    // If this node has a replica of that partition then Accept the connection
    // Otherwise Proxy the connection to a node that has a replica of that partition (randomly choose)
}

func (relayHub *RelayHub) DisconnectRelay(relayID string) {
    // This operation needs to happen AFTER the cluster state is updated in the cluster control loop
}

func (relayHub *RelayHub) AddPartition() {
}

func (relayHub *RelayHub) RemovePartition() {
    // should disconnect all relays associated with this partition
}

// Cloud node sync process?