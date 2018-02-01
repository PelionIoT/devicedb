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
    Batch(ctx context.Context, bucket string, batch client.Batch) error
    Get(ctx context.Context, bucket string, keys []string) ([]*client.Entry, error)
    GetMatches(ctx context.Context, bucket string, keys []string) (EntryIterator, error)
}

type Config struct {
    ServerURI string
    TLSConfig *tls.Config
}

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