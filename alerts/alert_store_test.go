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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/PelionIoT/devicedb/alerts"
	. "github.com/PelionIoT/devicedb/storage"
	. "github.com/PelionIoT/devicedb/util"
)

var _ = Describe("AlertStore", func() {
	var (
		storageEngine StorageDriver
		alertStore    *AlertStoreImpl
	)

	BeforeEach(func() {
		storageEngine = MakeNewStorageDriver()
		storageEngine.Open()

		alertStore = NewAlertStore(storageEngine)
	})

	AfterEach(func() {
		storageEngine.Close()
	})

	It("Should work", func() {
		Expect(alertStore.Put(Alert{Key: "abc"})).Should(BeNil())
		Expect(alertStore.Put(Alert{Key: "def"})).Should(BeNil())
		Expect(alertStore.Put(Alert{Key: "ghi"})).Should(BeNil())

		var alerts map[string]Alert = make(map[string]Alert)

		alertStore.ForEach(func(alert Alert) {
			alerts[alert.Key] = alert
		})

		Expect(alerts).Should(Equal(map[string]Alert{
			"abc": Alert{Key: "abc"},
			"def": Alert{Key: "def"},
			"ghi": Alert{Key: "ghi"},
		}))

		Expect(alertStore.DeleteAll(map[string]Alert{"abc": Alert{Key: "abc"}})).Should(BeNil())

		alerts = make(map[string]Alert)

		alertStore.ForEach(func(alert Alert) {
			alerts[alert.Key] = alert
		})

		Expect(alerts).Should(Equal(map[string]Alert{
			"def": Alert{Key: "def"},
			"ghi": Alert{Key: "ghi"},
		}))
	})
})
