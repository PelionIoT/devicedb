package routes_test
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
    "fmt"
    "net/http"
    "net/url"
    "time"

    . "github.com/armPelionEdge/devicedb/routes"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"

    "github.com/gorilla/mux"
    "github.com/gorilla/websocket"
    "github.com/onsi/gomega/ghttp"
)

var _ = Describe("Sync", func() {
    var router *mux.Router
    var syncEndpoint *SyncEndpoint
    var clusterFacade *MockClusterFacade
    var server *ghttp.Server

    BeforeEach(func() {
        clusterFacade = &MockClusterFacade{ }
        router = mux.NewRouter()
        syncEndpoint = &SyncEndpoint{
            ClusterFacade: clusterFacade,
            Upgrader: websocket.Upgrader{
                ReadBufferSize:  1024,
                WriteBufferSize: 1024,
            },
        }
    
        syncEndpoint.Attach(router)
        server = ghttp.NewServer()
        server.AppendHandlers(func(w http.ResponseWriter, r *http.Request) {
            router.ServeHTTP(w, r)
        })
    })

    Describe("/sync", func() {
        Describe("GET", func() {
            It("Should call AcceptRelayConnection() on the node facade", func() {
                acceptRelayConnectionCalled := make(chan int, 1)
                clusterFacade.acceptRelayConnectionCB = func(conn *websocket.Conn) {
                    acceptRelayConnectionCalled <- 1
                }

                dialer := &websocket.Dialer{ }
                u, err := url.Parse(server.URL())

                Expect(err).Should(Not(HaveOccurred()))

                _, _, err = dialer.Dial(fmt.Sprintf("ws://%s/sync", u.Host), nil)

                Expect(err).Should(Not(HaveOccurred()))

                select {
                case <-acceptRelayConnectionCalled:
                case <-time.After(time.Second):
                    Fail("Should have invoked AcceptRelayConnection()")
                }
            })
        })
    })
})
