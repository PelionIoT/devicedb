package server

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
	"github.com/PelionIoT/devicedb/historian"
)

type event struct {
	Device    string      `json:"device"`
	Event     string      `json:"event"`
	Metadata  interface{} `json:"metadata"`
	Timestamp uint64      `json:"timestamp"`
}

func MakeeventsFromEvents(es []*historian.Event) []*event {
	var events []*event = make([]*event, len(es))

	for i, e := range es {
		var metadata interface{}

		if err := json.Unmarshal([]byte(e.Data), &metadata); err != nil {
			metadata = e.Data
		}

		events[i] = &event{
			Device:    e.SourceID,
			Event:     e.Type,
			Metadata:  metadata,
			Timestamp: e.Timestamp,
		}
	}

	return events
}
