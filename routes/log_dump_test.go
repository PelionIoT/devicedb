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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/PelionIoT/devicedb/routes"

	"github.com/gorilla/mux"
)

var _ = Describe("LogDump", func() {
	var router *mux.Router
	var logDumpEndpoint *LogDumpEndpoint
	var clusterFacade *MockClusterFacade

	BeforeEach(func() {
		clusterFacade = &MockClusterFacade{}
		router = mux.NewRouter()
		logDumpEndpoint = &LogDumpEndpoint{
			ClusterFacade: clusterFacade,
		}
		logDumpEndpoint.Attach(router)
	})

	Describe("/log_dump", func() {
		Describe("GET", func() {
			Context("When LocalLogDump() returns an error", func() {
				It("Should respond with status code http.StatusInternalServerError", func() {
					clusterFacade.defaultLocalLogDumpError = errors.New("Some error")

					req, err := http.NewRequest("GET", "/log_dump", nil)

					Expect(err).Should(BeNil())

					rr := httptest.NewRecorder()
					router.ServeHTTP(rr, req)

					Expect(rr.Code).Should(Equal(http.StatusInternalServerError))
				})
			})

			Context("When LocalLogDump() does not return an error", func() {
				It("Should respond with status code http.StatusOK", func() {
					req, err := http.NewRequest("GET", "/log_dump", nil)

					Expect(err).Should(BeNil())

					rr := httptest.NewRecorder()
					router.ServeHTTP(rr, req)

					Expect(rr.Code).Should(Equal(http.StatusOK))
				})

				It("Should respond with a body that is the JSON encoded log dupm returned by LogDump()", func() {
					clusterFacade.defaultLocalLogDumpResponse = LogDump{
						BaseSnapshot:    LogSnapshot{Index: 22},
						Entries:         []LogEntry{},
						CurrentSnapshot: LogSnapshot{Index: 23},
					}

					req, err := http.NewRequest("GET", "/log_dump", nil)

					Expect(err).Should(BeNil())

					rr := httptest.NewRecorder()
					router.ServeHTTP(rr, req)

					var logDump LogDump

					Expect(json.Unmarshal(rr.Body.Bytes(), &logDump)).Should(BeNil())
					Expect(logDump).Should(Equal(clusterFacade.defaultLocalLogDumpResponse))
				})
			})
		})
	})
})
