package logging

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
