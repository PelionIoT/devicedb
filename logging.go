package devicedb

import (
    "os"
    "github.com/op/go-logging"
    "strings"
)

var Log = logging.MustGetLogger("devicedb")
var log = Log
var loggingBackend logging.LeveledBackend

func init() {
    var format = logging.MustStringFormatter(`%{color}%{time:15:04:05.000} ▶ %{level:.4s} %{shortfile}%{color:reset} %{message}`)
    var backend = logging.NewLogBackend(os.Stdout, "", 0)
    backendFormatter := logging.NewBackendFormatter(backend, format)
    loggingBackend = logging.AddModuleLevel(backendFormatter)
    
    logging.SetBackend(loggingBackend)
}

func SetLoggingLevel(ll string) {
    logLevel, err := logging.LogLevel(strings.ToUpper(ll))
    
    if err != nil {
        logLevel = logging.ERROR
    }
    
    loggingBackend.SetLevel(logLevel, "")
}