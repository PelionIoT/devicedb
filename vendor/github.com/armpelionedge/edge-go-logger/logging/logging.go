package logging
//
// Copyright (c) 2018, Arm Limited and affiliates.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


import (
    "os"
    "github.com/op/go-logging"
    "strings"
    "sync"
)

type splitLogBackend struct {
    rwMu sync.RWMutex
    outLogBackend logging.LeveledBackend
    errLogBackend logging.LeveledBackend
}

func newSplitLogBackend(outLogBackend, errLogBackend logging.LeveledBackend) *splitLogBackend {
    return &splitLogBackend{
        outLogBackend: outLogBackend,
        errLogBackend: errLogBackend,
    }
}

func (slb *splitLogBackend) Log(level logging.Level, calldepth int, rec *logging.Record) error {
    // Uses RWMutex so that calls to Log can happen concurrently with each other but not
    // with updates to the log level
    slb.rwMu.RLock()
    defer slb.rwMu.RUnlock()

    if level <= logging.WARNING {
        return slb.errLogBackend.Log(level, calldepth + 2, rec)
    }
    
    return slb.outLogBackend.Log(level, calldepth + 2, rec)
}

func (slb *splitLogBackend) SetLevel(level logging.Level, module string) {
    slb.rwMu.Lock()
    defer slb.rwMu.Unlock()

    slb.outLogBackend.SetLevel(level, module)
    slb.errLogBackend.SetLevel(level, module)
}

var Log = logging.MustGetLogger("")
var log = Log
var loggingBackend *splitLogBackend

func init() {
    var format = logging.MustStringFormatter(`%{color}%{time:15:04:05.000} ▶ %{level:.4s} %{shortfile}%{color:reset} %{message}`)
    var outBackend = logging.NewLogBackend(os.Stdout, "", 0)
    var outBackendFormatter = logging.NewBackendFormatter(outBackend, format)
    var outLogBackend = logging.AddModuleLevel(outBackendFormatter)
    var errBackend = logging.NewLogBackend(os.Stderr, "", 0)
    var errBackendFormatter = logging.NewBackendFormatter(errBackend, format)
    var errLogBackend = logging.AddModuleLevel(errBackendFormatter)
    
    loggingBackend = newSplitLogBackend(outLogBackend, errLogBackend)
    
    logging.SetBackend(loggingBackend)
    
	go watchLoggingConfig()
}

func LogLevelIsValid(ll string) bool {
    _, err := logging.LogLevel(strings.ToUpper(ll))

    return err == nil
}

func SetLoggingLevel(ll string) {
    logLevel, err := logging.LogLevel(strings.ToUpper(ll))
    
    if err != nil {
        logLevel = logging.ERROR
    }
    
    loggingBackend.SetLevel(logLevel, "")
}
