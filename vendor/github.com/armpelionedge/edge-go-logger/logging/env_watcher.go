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
	"io/ioutil"
	"os"
	"time"
)

const (
	// debug, info, notice, warning, error, critical
	LogLevelEnvironmentVariable string = "WIGWAG_LOG_LEVEL"
	LogLevelSyncPeriodSeconds int = 1
)

// Checks the log level environment variable periodically for changes
// update running log level if necessary
func watchLoggingConfig() {
	var logLevelSetting string

	for {
		time.Sleep(time.Second * time.Duration(LogLevelSyncPeriodSeconds))		
		
		var logLevelSettingFile string = os.Getenv(LogLevelEnvironmentVariable)

		if logLevelSettingFile == "" {
			continue
		}

		contents, err := ioutil.ReadFile(logLevelSettingFile)

		if err != nil {
			Log.Errorf("Unable to retrieve log level from %s: %v", logLevelSettingFile, err)
			
			continue
		}

		var newLogLevelSetting string = string(contents)

		if logLevelSetting != newLogLevelSetting && LogLevelIsValid(newLogLevelSetting) {
			Log.Debugf("Setting logging level to %s", newLogLevelSetting)

			logLevelSetting = newLogLevelSetting

			SetLoggingLevel(newLogLevelSetting)
		}
	}
}
