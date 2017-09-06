package transfer

import (
    "errors"
    "context"
    "net/http"
    "io"

    . "devicedb/cluster"
)

var EBadResponse = errors.New("Node responded with a bad response")

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
    peerAddress := transferTransport.configController.ClusterController().ClusterMemberAddress(nodeID)

    if peerAddress.IsEmpty() {
        return nil, nil, ENoSuchNode
    }

    endpointURL := peerAddress.ToHTTPURL("/partitions/{partitionID}/keys")
    request, err := http.NewRequest("GET", endpointURL, nil)

    if err != nil {
        return nil, nil, err
    }

    ctx, cancel := context.WithCancel(context.Background())
    request.WithContext(ctx)

    resp, err := transferTransport.httpClient.Do(request)

    if err != nil {
        cancel()
        resp.Body.Close()

        return nil, nil, err
    }

    if resp.StatusCode != http.StatusOK {
        cancel()
        resp.Body.Close()

        return nil, nil, EBadResponse
    }
    
    close := func() {
        // should do any cleanup on behalf of this request
        cancel()
        resp.Body.Close()
    }

    return resp.Body, close, nil
}