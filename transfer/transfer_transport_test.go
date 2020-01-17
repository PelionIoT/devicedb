package transfer_test
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
    "strings"
    "net/http"
    "time"

    . "github.com/armPelionEdge/devicedb/cluster"
    . "github.com/armPelionEdge/devicedb/transfer"
    . "github.com/armPelionEdge/devicedb/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("TransferTransport", func() {
    Describe("HTTPTransferTransport", func() {
        Describe("#Get", func() {
            Context("The specified node does not exist in the local config controller", func() {
                It("should return a nil reader, a nil cancel function, and an ENoSuchNode error", func() {
                    clusterController := &ClusterController{
                        LocalNodeID: 1,
                        State: ClusterState{
                            Nodes: map[uint64]*NodeConfig{
                                1: &NodeConfig{
                                    Address: PeerAddress{
                                        NodeID: 1,
                                        Host: "localhost",
                                        Port: 8080,
                                    },
                                },
                            },
                        },
                    }
                    configController := NewConfigController(nil, nil, clusterController)
                    httpClient := &http.Client{}
                    transferTransport := NewHTTPTransferTransport(configController, httpClient)

                    r, cancel, err := transferTransport.Get(2, 0)

                    Expect(r).Should(BeNil())
                    Expect(cancel).Should(BeNil())
                    Expect(err).Should(Equal(ENoSuchNode))
                })
            })

            Context("The HTTP request responds with 200", func() {
                It("should return a non-nil reader, a non-nil cancel function, and a nil error", func() {
                    handler := &StringResponseHandler{ str: strings.NewReader("HELLO") }
                    testServer := NewHTTPTestServer(8080, handler)
                    clusterController := &ClusterController{
                        LocalNodeID: 1,
                        State: ClusterState{
                            Nodes: map[uint64]*NodeConfig{
                                1: &NodeConfig{
                                    Address: PeerAddress{
                                        NodeID: 1,
                                        Host: "localhost",
                                        Port: 8080,
                                    },
                                },
                            },
                        },
                    }
                    configController := NewConfigController(nil, nil, clusterController)
                    httpClient := &http.Client{}
                    transferTransport := NewHTTPTransferTransport(configController, httpClient)

                    testServer.Start()
                    // give it enough time to fully start
                    <-time.After(time.Second)
                    r, cancel, err := transferTransport.Get(1, 0)

                    Expect(r).Should(Not(BeNil()))
                    Expect(cancel).Should(Not(BeNil()))
                    Expect(err).Should(BeNil())

                    cancel()
                    testServer.Stop()
                })

                Specify("The cancel function should cancel the http request and close the reader", func() {
                    infiniteReader := NewInfiniteReader()
                    handler := &StringResponseHandler{ str: infiniteReader, after: make(chan int) }
                    testServer := NewHTTPTestServer(8080, handler)
                    clusterController := &ClusterController{
                        LocalNodeID: 1,
                        State: ClusterState{
                            Nodes: map[uint64]*NodeConfig{
                                1: &NodeConfig{
                                    Address: PeerAddress{
                                        NodeID: 1,
                                        Host: "localhost",
                                        Port: 8080,
                                    },
                                },
                            },
                        },
                    }
                    configController := NewConfigController(nil, nil, clusterController)
                    httpClient := &http.Client{}
                    transferTransport := NewHTTPTransferTransport(configController, httpClient)

                    testServer.Start()
                    // give it enough time to fully start
                    <-time.After(time.Second)
                    r, cancel, err := transferTransport.Get(1, 0)

                    Expect(r).Should(Not(BeNil()))
                    Expect(cancel).Should(Not(BeNil()))
                    Expect(err).Should(BeNil())

                    // after should not complete until cancel is called.
                    go func() {
                        cancel()
                    }()

                    <-handler.after
                    Expect(handler.err).Should(Not(BeNil()))

                    testServer.Stop()
                })
            })

            Context("The HTTP request responds with a non-200 status code", func() {
                It("should return a nil reader, a nil cancel function, and a non-nil error", func() {
                    handler := &StringResponseHandler{ status: 400, str: strings.NewReader("") }
                    testServer := NewHTTPTestServer(8080, handler)
                    clusterController := &ClusterController{
                        LocalNodeID: 1,
                        State: ClusterState{
                            Nodes: map[uint64]*NodeConfig{
                                1: &NodeConfig{
                                    Address: PeerAddress{
                                        NodeID: 1,
                                        Host: "localhost",
                                        Port: 8080,
                                    },
                                },
                            },
                        },
                    }
                    configController := NewConfigController(nil, nil, clusterController)
                    httpClient := &http.Client{}
                    transferTransport := NewHTTPTransferTransport(configController, httpClient)

                    testServer.Start()
                    // give it enough time to fully start
                    <-time.After(time.Second)
                    r, cancel, err := transferTransport.Get(1, 0)

                    Expect(r).Should(BeNil())
                    Expect(cancel).Should(BeNil())
                    Expect(err).Should(Not(BeNil()))

                    testServer.Stop()
                })
            })

            Context("The HTTP request encounters an error before receiving a response", func() {
                It("should return a nil reader, a nil cancel function, and a non-nil error", func() {
                    clusterController := &ClusterController{
                        LocalNodeID: 1,
                        State: ClusterState{
                            Nodes: map[uint64]*NodeConfig{
                                1: &NodeConfig{
                                    Address: PeerAddress{
                                        NodeID: 1,
                                        Host: "localhost",
                                        Port: 8080,
                                    },
                                },
                            },
                        },
                    }
                    configController := NewConfigController(nil, nil, clusterController)
                    httpClient := &http.Client{}
                    transferTransport := NewHTTPTransferTransport(configController, httpClient)

                    // don't start server so there is an error with the get
                    r, cancel, err := transferTransport.Get(1, 0)

                    Expect(r).Should(BeNil())
                    Expect(cancel).Should(BeNil())
                    Expect(err).Should(Not(BeNil()))
                })
            })
        })
    })
})
