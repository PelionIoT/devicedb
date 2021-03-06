package alerts_test
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
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/armPelionEdge/devicedb/alerts"
)

var _ = Describe("AlertMap", func() {
	var alertMap *AlertMap
	var alertStore *MockAlertStore

	BeforeEach(func() {
		alertStore = NewMockAlertStore()
		alertMap = NewAlertMap(alertStore)
	})

	Describe("#UpdateAlert", func() {
		It("Should call Put() on the AlertStore with the same alert", func() {
			Expect(alertStore.Has("abc")).Should(BeFalse())
			alertMap.UpdateAlert(Alert{ Key: "abc", Timestamp: 123 })
			Expect(alertStore.Has("abc")).Should(BeTrue())
			Expect(alertStore.Get("abc")).Should(Equal(Alert{ Key: "abc", Timestamp: 123 }))
		})

		Context("And if AlertStore.Put() returns an error", func() {
			BeforeEach(func() {
				alertStore.putError = errors.New("Some error")
			})

			It("Should return an error as well", func() {
				Expect(alertMap.UpdateAlert(Alert{ Key: "abc", Timestamp: 456 })).Should(HaveOccurred())
			})
		})

		Context("And if AlertStore.Put() does not return an error", func() {
			It("Should not return an error either", func() {
				Expect(alertMap.UpdateAlert(Alert{ Key: "abc", Timestamp: 456 })).Should(Not(HaveOccurred()))
			})
		})
	})

	Describe("#GetAlerts", func() {
		BeforeEach(func() {
			alertStore.Put(Alert{ Key: "abc" })
			alertStore.Put(Alert{ Key: "def" })
			alertStore.Put(Alert{ Key: "ghi" })
		})

		Context("When AlertStore.ForEach() returns an error", func() {
			BeforeEach(func() {
				alertStore.forEachError = errors.New("Some error")
			})

			It("Should return an error", func() {
				alerts, err := alertMap.GetAlerts()

				Expect(alerts).Should(BeNil())
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("When AlertStore.ForEach() returns no error", func() {
			It("Should return a list of Alerts containing all alerts in the AlertStore", func() {
				alerts, err := alertMap.GetAlerts()

				Expect(alerts).Should(HaveLen(3))
				Expect(alerts["abc"]).Should(Equal(Alert{ Key: "abc" }))
				Expect(alerts["def"]).Should(Equal(Alert{ Key: "def" }))
				Expect(alerts["ghi"]).Should(Equal(Alert{ Key: "ghi" }))
				Expect(err).Should(BeNil())
			})
		})
	})

	Describe("#ClearAlerts", func() {
		Context("When AlertStore.DeleteAlerts() return an error", func() {
			BeforeEach(func() {
				alertStore.deleteAllError = errors.New("Some error")
			})

			It("Should return an error", func() {
				Expect(alertMap.ClearAlerts(map[string]Alert{ })).Should(HaveOccurred())
			})
		})

		Context("When none of the alerts passed into ClearAlerts() exist in the AlertStore", func() {
			BeforeEach(func() {
				alertStore.Put(Alert{ Key: "abc", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "def", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "ghi", Timestamp: 2 })
			})

			It("Should not delete anything", func() {
				Expect(alertMap.ClearAlerts(map[string]Alert{
					"x": Alert{ Key: "z" },
					"y": Alert{ Key: "y" },
					"z": Alert{ Key: "z" },
				})).Should(BeNil())

				Expect(alertStore.Get("abc")).Should(Equal(Alert{ Key: "abc", Timestamp: 2 }))
				Expect(alertStore.Get("def")).Should(Equal(Alert{ Key: "def", Timestamp: 2 }))
				Expect(alertStore.Get("ghi")).Should(Equal(Alert{ Key: "ghi", Timestamp: 2 }))
			})
		})

		Context("When all alerts passed into ClearAlerts() exist in the AlertStore but they have been updated", func() {
			BeforeEach(func() {
				alertStore.Put(Alert{ Key: "abc", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "def", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "ghi", Timestamp: 2 })
			})

			It("Should not delete anything", func() {
				Expect(alertMap.ClearAlerts(map[string]Alert{
					"abc": Alert{ Key: "abc", Timestamp: 1 },
					"def": Alert{ Key: "def", Timestamp: 1 },
					"ghi": Alert{ Key: "ghi", Timestamp: 1 },
				})).Should(BeNil())

				Expect(alertStore.Get("abc")).Should(Equal(Alert{ Key: "abc", Timestamp: 2 }))
				Expect(alertStore.Get("def")).Should(Equal(Alert{ Key: "def", Timestamp: 2 }))
				Expect(alertStore.Get("ghi")).Should(Equal(Alert{ Key: "ghi", Timestamp: 2 }))
			})
		})

		Context("When some alerts passed into ClearAlerts() exist in the AlertStore and they have not been updated", func() {
			BeforeEach(func() {
				alertStore.Put(Alert{ Key: "abc", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "def", Timestamp: 2 })
				alertStore.Put(Alert{ Key: "ghi", Timestamp: 2 })
			})

			It("Should delete those alerts in the AlertStore that have not been updated", func() {
				Expect(alertMap.ClearAlerts(map[string]Alert{
					"abc": Alert{ Key: "abc", Timestamp: 2 },
					"def": Alert{ Key: "def", Timestamp: 2 },
					"ghi": Alert{ Key: "ghi", Timestamp: 2 },
				})).Should(BeNil())

				Expect(alertStore.Has("abc")).Should(BeFalse())
				Expect(alertStore.Has("def")).Should(BeFalse())
				Expect(alertStore.Has("ghi")).Should(BeFalse())
			})
		})
	})
})
