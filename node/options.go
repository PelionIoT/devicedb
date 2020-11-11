package node
//
 // Copyright (c) 2019 ARM Limited.
 //
 // SPDX-License-Identifier: MIT
 //
 // Permission is hereby granted, free of charge, to any person obtaining a copy
 // of this software and associated documentation files (the "Software"), to
 // deal in the Software without restriction, including without limitation the
 // rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 // sell copies of the Software, and to permit persons to whom the Software is
 // furnished to do so, subject to the following conditions:
 //
 // The above copyright notice and this permission notice shall be included in all
 // copies or substantial portions of the Software.
 //
 // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 // SOFTWARE.
 //


import (
    . "github.com/PelionIoT/devicedb/cluster"
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
    SyncMaxSessions uint
    SyncPathLimit uint32
    SyncPeriod uint
    SnapshotDirectory string
}

func (options NodeInitializationOptions) SnapshotsEnabled() bool {
    return options.SnapshotDirectory != ""
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