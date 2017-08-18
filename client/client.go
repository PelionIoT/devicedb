package client

import (
    "errors"
    "net/http"
    "bytes"
    "io/ioutil"
    "encoding/json"
    "time"
    "strings"
    "context"

    . "devicedb/raft"
    . "devicedb/cluster"
)

const DefaultClientTimeout = time.Second * 10

type ErrorStatusCode struct {
    StatusCode int
    Message string
}

func (errorStatus *ErrorStatusCode) Error() string {
    return errorStatus.Message
}

type ClientConfig struct {
    Timeout time.Duration
}

var EClientTimeout = errors.New("Client request timed out")

type Client struct {
    httpClient *http.Client
}

func NewClient(config ClientConfig) *Client {
    if config.Timeout == 0 {
        config.Timeout = DefaultClientTimeout
    }

    return &Client{
        httpClient: &http.Client{ 
            Timeout: config.Timeout,
        },
    }
}

func (client *Client) sendRequest(ctx context.Context, httpVerb string, endpointURL string, body []byte) ([]byte, error) {
    request, err := http.NewRequest(httpVerb, endpointURL, bytes.NewReader(body))

    if err != nil {
        return nil, err
    }

    request = request.WithContext(ctx)

    resp, err := client.httpClient.Do(request)

    if err != nil {
        if strings.Contains(err.Error(), "Timeout") {
            return nil, EClientTimeout
        }

        return nil, err
    }
    
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        errorMessage, err := ioutil.ReadAll(resp.Body)
        
        if err != nil {
            return nil, err
        }
       
        return nil, &ErrorStatusCode{ Message: string(errorMessage), StatusCode: resp.StatusCode }
    }

    responseBody, err := ioutil.ReadAll(resp.Body)

    if err != nil {
        return nil, err
    }

    return responseBody, nil
}

// Use an existing cluster member to bootstrap the addition of another node
// to that cluster. host and port indicate the address of the existing cluster 
// member while nodeAddress contains the ID, host name and port of the new
// cluster member
//
// Return Values:
//   EClientTimeout: The request to the node timed out
func (client *Client) AddNode(ctx context.Context, memberAddress PeerAddress, newMemberConfig NodeConfig) error {
    encodedNodeConfig, _ := json.Marshal(newMemberConfig)
    _, err := client.sendRequest(ctx, "POST", memberAddress.ToHTTPURL("/cluster/nodes"), encodedNodeConfig)

    return err
}

// Ask a cluster member to initiate the removal of some node from its cluster.
// host and port indicate the address of the initiator node while nodeID is
// the ID of the node that should be removed.
//
// Return Values:
//   EClientTimeout: The request to the node timed out
func (client *Client) RemoveNode(ctx context.Context, member PeerAddress, nodeID uint64) error {
    return nil
}
