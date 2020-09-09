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
	"encoding/json"
	"github.com/armPelionEdge/devicedb/storage"
)

type AlertStoreImpl struct {
	storageDriver storage.StorageDriver
}

func NewAlertStore(storageDriver storage.StorageDriver) *AlertStoreImpl {
	return &AlertStoreImpl{
		storageDriver: storageDriver,
	}
}

func (alertStore *AlertStoreImpl) Put(alert Alert) error {
	encodedAlert, err := json.Marshal(alert)

	if err != nil {
		return err
	}

	batch := storage.NewBatch()
	batch.Put([]byte(alert.Key), encodedAlert)

	return alertStore.storageDriver.Batch(batch)
}

func (alertStore *AlertStoreImpl) DeleteAll(alerts map[string]Alert) error {
	batch := storage.NewBatch()

	for _, alert := range alerts {
		batch.Delete([]byte(alert.Key))
	}

	return alertStore.storageDriver.Batch(batch)
}

func (alertStore *AlertStoreImpl) ForEach(cb func(alert Alert)) error {
	iter, err := alertStore.storageDriver.GetMatches([][]byte{ []byte{ } })

	if err != nil {
		return err
	}

	defer iter.Release()

	for iter.Next() {
		var alert Alert

		if err := json.Unmarshal(iter.Value(), &alert); err != nil {
			return err
		}

		cb(alert)
	}

	return iter.Error()
}