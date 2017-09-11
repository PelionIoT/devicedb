package node

import (
    . "devicedb/cluster"
)

type NodeInitializationOptions struct {
    StartCluster bool
    JoinCluster bool
    ClusterSettings ClusterSettings
    SeedNodeHost string
    SeedNodePort int
    ClusterHost string
    ClusterPort int
    ExternalHost string
    ExternalPort int
}

func (options NodeInitializationOptions) ShouldStartCluster() bool {
    return options.StartCluster
}

func (options NodeInitializationOptions) ShouldJoinCluster() bool {
    return options.JoinCluster
}

func (options NodeInitializationOptions) ClusterAddress() (host string, port int) {
    return options.ClusterHost, options.ClusterPort
}

func (options NodeInitializationOptions) ExternalAddress() (host string, port int) {
    return options.ExternalHost, options.ExternalPort
}

func (options NodeInitializationOptions) SeedNode() (host string, port int) {
    return options.SeedNodeHost, options.SeedNodePort
}