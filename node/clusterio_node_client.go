package node

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/error"
    . "devicedb/raft"
    . "devicedb/routes"
)

type NodeClient struct {
    configController ClusterConfigController
    localNode Node
    httpClient *http.Client
}

func NewNodeClient(localNode Node, configController ClusterConfigController) *NodeClient {
    return &NodeClient{
        configController: configController,
        localNode: localNode,
        httpClient: &http.Client{ },
    }
}

func (nodeClient *NodeClient) Merge(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error {
    var nodeAddress PeerAddress = nodeClient.configController.ClusterController().ClusterMemberAddress(nodeID)

    if nodeAddress.IsEmpty() {
        return ENoSuchNode
    }

    if nodeID == nodeClient.localNode.ID() {
        err := nodeClient.localNode.Merge(ctx, partition, siteID, bucket, patch, broadcastToRelays)

        switch err {
        case ENoSuchBucket:
            return EBucketDoesNotExist
        case ENoSuchSite:
            return ESiteDoesNotExist
        case nil:
            return nil
        default:
            return err
        }
    }

    encodedPatch, err := json.Marshal(patch)

    if err != nil {
        return err
    }

    broadcastQuery := ""

    if broadcastToRelays {
        broadcastQuery = "?broadcast=true"
    }

    status, body, err := nodeClient.sendRequest(ctx, "POST", fmt.Sprintf("http://%s:%d/partitions/%d/sites/%s/buckets/%s/merges%s", nodeAddress.Host, nodeAddress.Port, partition, siteID, bucket, broadcastQuery), encodedPatch)

    if err != nil {
        return err
    }

    switch status {
    case 404:
        dbErr, err := DBErrorFromJSON(body)

        if err != nil {
            return err
        }

        return dbErr
    case 200:
        var batchResult BatchResult

        if err := json.Unmarshal(body, &batchResult); err != nil {
            return err
        }

        if batchResult.NApplied == 0 {
            return ENoQuorum
        }

        return nil
    default:
        return EStorage
    }
}

func (nodeClient *NodeClient) Batch(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) (map[string]*SiblingSet, error) {
    var nodeAddress PeerAddress = nodeClient.configController.ClusterController().ClusterMemberAddress(nodeID)

    if nodeAddress.IsEmpty() {
        return nil, ENoSuchNode
    }

    if nodeID == nodeClient.localNode.ID() {
        patch, err := nodeClient.localNode.Batch(ctx, partition, siteID, bucket, updateBatch)

        switch err {
        case ENoSuchBucket:
            return nil, EBucketDoesNotExist
        case ENoSuchSite:
            return nil, ESiteDoesNotExist
        case nil:
            return patch, nil
        default:
            return nil, err
        }
    }

    encodedUpdateBatch, err := updateBatch.ToJSON()

    if err != nil {
        return nil, err
    }

    status, body, err := nodeClient.sendRequest(ctx, "POST", fmt.Sprintf("http://%s:%d/partitions/%d/sites/%s/buckets/%s/batches", nodeAddress.Host, nodeAddress.Port, partition, siteID, bucket), encodedUpdateBatch)

    if err != nil {
        return nil, err
    }

    switch status {
    case 404:
        dbErr, err := DBErrorFromJSON(body)

        if err != nil {
            return nil, err
        }

        return nil, dbErr
    case 200:
        var batchResult BatchResult

        if err := json.Unmarshal(body, &batchResult); err != nil {
            return nil, err
        }

        if batchResult.NApplied == 0 {
            return nil, ENoQuorum
        }

        return batchResult.Patch, nil
    default:
        return nil, EStorage
    }
}

func (nodeClient *NodeClient) Get(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
    var nodeAddress PeerAddress = nodeClient.configController.ClusterController().ClusterMemberAddress(nodeID)

    if nodeAddress.IsEmpty() {
        return nil, ENoSuchNode
    }

    if nodeID == nodeClient.localNode.ID() {
        siblingSets, err := nodeClient.localNode.Get(ctx, partition, siteID, bucket, keys)

        switch err {
        case ENoSuchBucket:
            return nil, EBucketDoesNotExist
        case ENoSuchSite:
            return nil, ESiteDoesNotExist
        case nil:
            return siblingSets, nil
        default:
            return nil, err
        }
    }

    var queryString string

    for i, key := range keys {
        queryString += "key=" + string(key)

        if i != len(keys) - 1 {
            queryString += "&"
        }
    }

    status, body, err := nodeClient.sendRequest(ctx, "GET", fmt.Sprintf("http://%s:%d/partitions/%d/sites/%s/buckets/%s/keys?%s", nodeAddress.Host, nodeAddress.Port, partition, siteID, bucket, queryString), nil)

    if err != nil {
        return nil, err
    }

    switch status {
    case 404:
        dbErr, err := DBErrorFromJSON(body)

        if err != nil {
            return nil, err
        }

        return nil, dbErr
    case 200:
    default:
        return nil, EStorage
    }

    var entries []InternalEntry

    err = json.Unmarshal(body, &entries)

    if err != nil {
        return nil, err
    }

    var siblingSets []*SiblingSet = make([]*SiblingSet, len(entries))

    for i, entry := range entries {
        siblingSets[i] = entry.Siblings
    }

    return siblingSets, nil
}

func (nodeClient *NodeClient) GetMatches(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error) {
    var nodeAddress PeerAddress = nodeClient.configController.ClusterController().ClusterMemberAddress(nodeID)

    if nodeAddress.IsEmpty() {
        return nil, ENoSuchNode
    }

    if nodeID == nodeClient.localNode.ID() {
        iter, err := nodeClient.localNode.GetMatches(ctx, partition, siteID, bucket, keys)

        switch err {
        case ENoSuchBucket:
            return nil, EBucketDoesNotExist
        case ENoSuchSite:
            return nil, ESiteDoesNotExist
        case nil:
            return iter, nil
        default:
            return nil, err
        }
    }
    
    var queryString string

    for i, key := range keys {
        queryString += "prefix=" + string(key)

        if i != len(keys) - 1 {
            queryString += "&"
        }
    }

    status, body, err := nodeClient.sendRequest(ctx, "GET", fmt.Sprintf("http://%s:%d/partitions/%d/sites/%s/buckets/%s/keys?%s", nodeAddress.Host, nodeAddress.Port, partition, siteID, bucket, queryString), nil)

    if err != nil {
        return nil, err
    }

    switch status {
    case 404:
        dbErr, err := DBErrorFromJSON(body)

        if err != nil {
            return nil, err
        }

        return nil, dbErr
    case 200:
    default:
        return nil, EStorage
    }

    var entries []InternalEntry

    err = json.Unmarshal(body, &entries)

    if err != nil {
        return nil, err
    }

    return newInternalEntrySiblingSetIterator(entries), nil
}

func (nodeClient *NodeClient) sendRequest(ctx context.Context, httpVerb string, endpointURL string, body []byte) (int, []byte, error) {
    request, err := http.NewRequest(httpVerb, endpointURL, bytes.NewReader(body))

    if err != nil {
        return 0, nil, err
    }

    request = request.WithContext(ctx)

    resp, err := nodeClient.httpClient.Do(request)

    if err != nil {
        return 0, nil, err
    }
    
    defer resp.Body.Close()
    
    responseBody, err := ioutil.ReadAll(resp.Body)

    if err != nil {
        return 0, nil, err
    }

    return resp.StatusCode, responseBody, nil
}

type internalEntrySiblingSetIterator struct {
    entries []InternalEntry
    currentEntry int
}

func newInternalEntrySiblingSetIterator(entries []InternalEntry) *internalEntrySiblingSetIterator {
    return &internalEntrySiblingSetIterator{
        entries: entries,
        currentEntry: -1,
    }
}

func (iter *internalEntrySiblingSetIterator) Next() bool {
    if iter.currentEntry < len(iter.entries) {
        iter.currentEntry++
    }

    return iter.currentEntry < len(iter.entries)
}

func (iter *internalEntrySiblingSetIterator) Prefix() []byte {
    if iter.currentEntry < 0 || iter.currentEntry >= len(iter.entries) {
        return nil
    }

    return []byte(iter.entries[iter.currentEntry].Prefix)
}

func (iter *internalEntrySiblingSetIterator) Key() []byte {
    if iter.currentEntry < 0 || iter.currentEntry >= len(iter.entries) {
        return nil
    }

    return []byte(iter.entries[iter.currentEntry].Key)
}

func (iter *internalEntrySiblingSetIterator) Value() *SiblingSet {
    if iter.currentEntry < 0 || iter.currentEntry >= len(iter.entries) {
        return nil
    }

    return iter.entries[iter.currentEntry].Siblings
}

func (iter *internalEntrySiblingSetIterator) Release() {
}

func (iter *internalEntrySiblingSetIterator) Error() error {
    return nil
}