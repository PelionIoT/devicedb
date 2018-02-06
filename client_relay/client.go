package client_relay

import (
    "bytes"    
    "context"
    "crypto/tls"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "io/ioutil"
    "net/http"
    "devicedb/client"
    "devicedb/transport"
)

type Client interface {
    // Execute a batch update in DeviceDB. The context is bound to the
    // request. The bucket should be the name of the devicedb bucket to
    // which this update should be applied.
    Batch(ctx context.Context, bucket string, batch client.Batch) error
    // Get the value of one or more keys in devicedb. The bucket shold be
    // the name of the devicedb bucket to which this update should be applied.
    // The keys array describes which keys should be retrieved. If no error
    // occurs this function will return an array of values corresponding
    // to the keys that were requested. The results array will mirror the
    // keys array. In other words, the ith value in the result is the value
    // for key i. If a key does not exist the value will be nil.
    Get(ctx context.Context, bucket string, keys []string) ([]*client.Entry, error)
    // Get keys matching one or more prefixes. The keys array represents
    // a list of prefixes to query. The resulting iterator will iterate
    // through database values whose key matches one of the specified
    // prefixes
    GetMatches(ctx context.Context, bucket string, keys []string) (EntryIterator, error)
}

type Config struct {
    // The server URI is the base URI for the devicedb server
    // An example of this is https://localhost:9000
    ServerURI string
    // Provide a TLS config if you are connecting to a TLS
    // enabled devicedb relay server. You will need to provide
    // the relay CA and server name (the relay ID)
    TLSConfig *tls.Config
}

// Create a new DeviceDB client
func New(config Config) Client {
    return &HTTPClient{
        server: config.ServerURI,
        httpClient: &http.Client{ Transport: &http.Transport{ TLSClientConfig: config.TLSConfig } },
    }
}

type HTTPClient struct {
    server string
    httpClient *http.Client
}

func (c *HTTPClient) Batch(ctx context.Context, bucket string, batch client.Batch) error {
    var transportBatch transport.TransportUpdateBatch = batch.ToTransportUpdateBatch()    
    url := fmt.Sprintf("/%s/batch", bucket)

    body, err := json.Marshal(transportBatch)

    if err != nil {
        return err
    }

    respBody, err := c.sendRequest(ctx, "POST", url, body)

    if err != nil {
        return err
    }

    respBody.Close()

    return nil
}

func (c *HTTPClient) Get(ctx context.Context, bucket string, keys []string) ([]*client.Entry, error) {
    url := fmt.Sprintf("/%s/values?", bucket)
    body, err := json.Marshal(keys)

    if err != nil {
        return nil, err
    }

    respBody, err := c.sendRequest(ctx, "POST", url, body)

    if err != nil {
        return nil, err
    }

    defer respBody.Close()

    var decoder *json.Decoder = json.NewDecoder(respBody)
    var transportSiblingSets []*transport.TransportSiblingSet

    err = decoder.Decode(&transportSiblingSets)

    if err != nil {
        return nil, err
    }

    if len(transportSiblingSets) != len(keys) {
        return nil, errors.New(fmt.Sprintf("A protocol error occurred: Asked for %d keys but received %d values in the result array", len(keys), len(transportSiblingSets)))
    }

    var entries []*client.Entry = make([]*client.Entry, len(transportSiblingSets))

    for i, _ := range keys {
        if transportSiblingSets[i] == nil {
            continue
        }

        entries[i] = &client.Entry{
            Context: transportSiblingSets[i].Context,
            Siblings: transportSiblingSets[i].Siblings,
        }
    }

    return entries, nil
}

func (c *HTTPClient) GetMatches(ctx context.Context, bucket string, keys []string) (EntryIterator, error) {
    url := fmt.Sprintf("/%s/matches?", bucket)
    body, err := json.Marshal(keys)

    if err != nil {
        return nil, err
    }

    respBody, err := c.sendRequest(ctx, "POST", url, body)

    if err != nil {
        return nil, err
    }

    return &StreamedEntryIterator{ reader: respBody }, nil
}

func (c *HTTPClient) sendRequest(ctx context.Context, httpVerb string, endpointURL string, body []byte) (io.ReadCloser, error) {
    u := fmt.Sprintf("%s%s", c.server, endpointURL)
    request, err := http.NewRequest(httpVerb, u, bytes.NewReader(body))

    if err != nil {
        return nil, err
    }

    request = request.WithContext(ctx)

    resp, err := c.httpClient.Do(request)

    if err != nil {
        return nil, err
    }
    
    if resp.StatusCode != http.StatusOK {
        errorMessage, err := ioutil.ReadAll(resp.Body)

        resp.Body.Close()
        
        if err != nil {
            return nil, err
        }
       
        return nil, &client.ErrorStatusCode{ Message: string(errorMessage), StatusCode: resp.StatusCode }
    }

    return resp.Body, nil
}