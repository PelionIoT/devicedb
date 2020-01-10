package alerts
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
	"sync"
)

type AlertStore interface {
	Put(alert Alert) error
	DeleteAll(alerts map[string]Alert) error
	ForEach(func(alert Alert)) error
}

type AlertMap struct {
	mu sync.Mutex
	alertStore AlertStore
}

func NewAlertMap(alertStore AlertStore) *AlertMap {
	return &AlertMap{
		alertStore: alertStore,
	}
}

func (alertMap *AlertMap) UpdateAlert(alert Alert) error {
	alertMap.mu.Lock()
	defer alertMap.mu.Unlock()

	return alertMap.alertStore.Put(alert)
}

func (alertMap *AlertMap) GetAlerts() (map[string]Alert, error) {
	var alerts map[string]Alert = make(map[string]Alert)

	err := alertMap.alertStore.ForEach(func(alert Alert) {
		alerts[alert.Key] = alert
	})

	if err != nil {
		return nil, err
	}

	return alerts, nil
}

// Blocks calls to UpdateAlert()
func (alertMap *AlertMap) ClearAlerts(alerts map[string]Alert) error {
	alertMap.mu.Lock()
	defer alertMap.mu.Unlock()

	var deleteAlerts map[string]Alert = make(map[string]Alert, len(alerts))

	for _, a := range alerts {
		deleteAlerts[a.Key] = a
	}

	err := alertMap.alertStore.ForEach(func(alert Alert) {
		if a, ok := alerts[alert.Key]; ok && alert.Timestamp != a.Timestamp {
			// This shouldn't be deleted since its value was changed since
			// reading. The new value will need to be forwarded later
			delete(deleteAlerts, a.Key)
		}
	})

	if err != nil {
		return err
	}

	return alertMap.alertStore.DeleteAll(deleteAlerts)
}