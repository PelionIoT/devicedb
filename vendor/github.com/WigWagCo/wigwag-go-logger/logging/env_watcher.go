package logging

import (
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
func watchLoggingEnvVariable() {
	var logLevelSetting string

	for {
		<-time.After(time.Second * time.Duration(LogLevelSyncPeriodSeconds))

		var newLogLevelSetting string = os.Getenv(LogLevelEnvironmentVariable)

		if logLevelSetting != newLogLevelSetting && LogLevelIsValid(newLogLevelSetting) {
			Log.Debugf("Setting logging level to %s", newLogLevelSetting)

			logLevelSetting = newLogLevelSetting

			SetLoggingLevel(newLogLevelSetting)
		}
	}
}
