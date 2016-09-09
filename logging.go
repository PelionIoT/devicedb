package devicedb

import (
    "os"
    "github.com/op/go-logging"
)

var log = logging.MustGetLogger("devicedb")

func init() {
    var format = logging.MustStringFormatter(`%{color}%{time:15:04:05.000} ▶ %{level:.4s} %{shortfile}%{color:reset} %{message}`)
    var backend = logging.NewLogBackend(os.Stdout, "", 0)
    backendFormatter := logging.NewBackendFormatter(backend, format)

    logging.SetBackend(backendFormatter)
}