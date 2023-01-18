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
	. "github.com/PelionIoT/devicedb/alerts"
)

type MockAlertStore struct {
	alerts         map[string]Alert
	putError       error
	deleteAllError error
	forEachError   error
}

func NewMockAlertStore() *MockAlertStore {
	return &MockAlertStore{
		alerts: make(map[string]Alert),
	}
}

func (alertStore *MockAlertStore) Put(alert Alert) error {
	if alertStore.putError != nil {
		return alertStore.putError
	}

	alertStore.alerts[alert.Key] = alert

	return nil
}

func (alertStore *MockAlertStore) Has(key string) bool {
	_, ok := alertStore.alerts[key]

	return ok
}

func (alertStore *MockAlertStore) Get(key string) Alert {
	return alertStore.alerts[key]
}

func (alertStore *MockAlertStore) DeleteAll(alerts map[string]Alert) error {
	if alertStore.deleteAllError != nil {
		return alertStore.deleteAllError
	}

	for _, a := range alerts {
		delete(alertStore.alerts, a.Key)
	}

	return nil
}

func (alertStore *MockAlertStore) ForEach(cb func(alert Alert)) error {
	if alertStore.forEachError != nil {
		return alertStore.forEachError
	}

	for _, a := range alertStore.alerts {
		cb(a)
	}

	return nil
}
