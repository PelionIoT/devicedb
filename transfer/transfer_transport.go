package transfer

import (
    "net/http"
    "io"

    . "devicedb/cluster"
)

type PartitionTransferTransport interface {
    Get(nodeID uint64, partition uint64) (io.Reader, func(), error)
}

type HTTPTransferTransport struct {
    httpClient *http.Client
    configController *ConfigController
}

func NewHTTPTransferTransport(configController *ConfigController, httpClient *http.Client) *HTTPTransferTransport {
    return &HTTPTransferTransport{
        httpClient: httpClient,
        configController: configController,
    }
}

func (transferTransport *HTTPTransferTransport) Get(nodeID uint64, partition uint64) (io.Reader, func(), error) {
    return nil, nil, nil
}