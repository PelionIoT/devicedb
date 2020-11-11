package transfer
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
    "context"
    "errors"
    "fmt"
    "net/http"
    "io"

    . "github.com/PelionIoT/devicedb/cluster"
)

const DefaultEndpointURL = "/partitions/%d/keys"
var EBadResponse = errors.New("Node responded with a bad response")

type PartitionTransferTransport interface {
    Get(nodeID uint64, partition uint64) (io.Reader, func(), error)
}

type HTTPTransferTransport struct {
    httpClient *http.Client
    configController ClusterConfigController
    endpointURL string
}

func NewHTTPTransferTransport(configController ClusterConfigController, httpClient *http.Client) *HTTPTransferTransport {
    return &HTTPTransferTransport{
        httpClient: httpClient,
        configController: configController,
        endpointURL: DefaultEndpointURL,
    }
}

func (transferTransport *HTTPTransferTransport) SetEndpointURL(endpointURL string) *HTTPTransferTransport {
    transferTransport.endpointURL = endpointURL

    return transferTransport
}

func (transferTransport *HTTPTransferTransport) Get(nodeID uint64, partition uint64) (io.Reader, func(), error) {
    peerAddress := transferTransport.configController.ClusterController().ClusterMemberAddress(nodeID)

    if peerAddress.IsEmpty() {
        return nil, nil, ENoSuchNode
    }

    endpointURL := peerAddress.ToHTTPURL(fmt.Sprintf(transferTransport.endpointURL, partition))
    request, err := http.NewRequest("GET", endpointURL, nil)

    if err != nil {
        return nil, nil, err
    }

    ctx, cancel := context.WithCancel(context.Background())
    request.WithContext(ctx)

    resp, err := transferTransport.httpClient.Do(request)

    if err != nil {
        cancel()

        if resp != nil && resp.Body != nil {
            resp.Body.Close()
        }

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