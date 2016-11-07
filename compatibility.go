// This file provides utility functions that make backwards compatibility with original DeviceDB (written in NodeJS)
// easier
package devicedb

import (
    "encoding/json"
    "sort"
    "strings"
    "errors"
    "fmt"
)

func SiblingToNormalizedJSON(sibling *Sibling) string {
    return valueToJSON(sibling.Value()) + dotToJSON(sibling.Clock().Dot()) + contextToJSON(sibling.Clock().Context())
}

func valueToJSON(value []byte) string {
    if value == nil {
        return "null"
    }
    
    j, _ := json.Marshal(string(value))
    
    return string(j)
}

func dotToJSON(dot Dot) string {
    nodeIDJSON, _ := json.Marshal(dot.NodeID)
    countJSON, _ := json.Marshal(dot.Count)
    
    return "[" + string(nodeIDJSON) + "," + string(countJSON) + "]"
}

func contextToJSON(context map[string]uint64) string {
    keys := make([]string, 0, len(context))
    
    for k, _ := range context {
        keys = append(keys, k)
    }
    
    sort.Strings(keys)
    
    j := "["
    
    for i := 0; i < len(keys); i += 1 {
        nodeIDJSON, _ := json.Marshal(keys[i])
        countJSON, _ := json.Marshal(context[keys[i]])
        
        j += "[[" + string(nodeIDJSON) + "," + string(countJSON) + "],null]"
        
        if i != len(keys) - 1 {
            j += ","
        }
    }
    
    j += "]"
    
    return j
}

func HashSiblingSet(key string, siblingSet *SiblingSet) Hash {
    siblingsJSON := make([]string, 0, siblingSet.Size())
    
    for sibling := range siblingSet.Iter() {
        siblingsJSON = append(siblingsJSON, SiblingToNormalizedJSON(sibling))
    }
    
    sort.Strings(siblingsJSON)
    
    j, _ := json.Marshal(siblingsJSON)
    
    return NewHash([]byte(key + string(j)))
}

func UpgradeLegacyDatabase(legacyDatabasePath string, serverConfig YAMLServerConfig) error {
    bucketDataPrefix := "cache."
    bucketNameMapping := map[string]string {
        "shared": "default",
        "lww": "lww",
        "cloud": "cloud",
        "local": "local",
    }
    
    legacyDatabaseDriver := NewLevelDBStorageDriver(legacyDatabasePath, nil)
    newDBStorageDriver := NewLevelDBStorageDriver(serverConfig.DBFile, nil)
    bucketList := NewBucketList()
    
    err := newDBStorageDriver.Open()
    
    if err != nil {
        return err
    }
    
    defer newDBStorageDriver.Close()
    
    err = legacyDatabaseDriver.Open()
    
    if err != nil {
        return err
    }
    
    defer legacyDatabaseDriver.Close()
    
    defaultNode, _ := NewNode("", NewPrefixedStorageDriver([]byte{ 0 }, newDBStorageDriver), serverConfig.MerkleDepth, nil)
    cloudNode, _ := NewNode("", NewPrefixedStorageDriver([]byte{ 1 }, newDBStorageDriver), serverConfig.MerkleDepth, nil) 
    lwwNode, _ := NewNode("", NewPrefixedStorageDriver([]byte{ 2 }, newDBStorageDriver), serverConfig.MerkleDepth, LastWriterWins)
    
    bucketList.AddBucket("default", defaultNode, &Shared{ })
    bucketList.AddBucket("lww", lwwNode, &Shared{ })
    bucketList.AddBucket("cloud", cloudNode, &Cloud{ })
    
    iter, err := legacyDatabaseDriver.GetMatches([][]byte{ []byte(bucketDataPrefix) })
    
    if err != nil {
        return err
    }
    
    for iter.Next() {
        key := iter.Key()[len(iter.Prefix()):]
        value := iter.Value()
        keyParts := strings.Split(string(key), ".")
        
        if len(keyParts) < 2 {
            iter.Release()
            
            return errors.New(fmt.Sprintf("Key was invalid: %s", key))
        }
        
        legacyBucketName := keyParts[0]
        newBucketName, ok := bucketNameMapping[legacyBucketName]
        
        if !ok {
            log.Warningf("Cannot translate object at %s because %s is not a recognized bucket name", key, legacyBucketName)
            
            continue
        }
        
        if !bucketList.HasBucket(newBucketName) {
            log.Warningf("Cannot translate object at %s because %s is not a recognized bucket name", key, newBucketName)
            
            continue
        }
        
        node := bucketList.Get(newBucketName).Node
        siblingSet, err := DecodeLegacySiblingSet(value)
        
        if err != nil {
            log.Warningf("Unable to decode object at %s (%s): %v", key, string(value), err)
            
            continue
        }
    
        nonPrefixedKey := string(key)[len(legacyBucketName) + 1:]
        err = node.Merge(map[string]*SiblingSet{
            nonPrefixedKey: siblingSet,
        })
        
        if err != nil {
            log.Warningf("Unable to migrate object at %s (%s): %v", key, string(value), err)
        } else {
            log.Debugf("Migrated object in legacy bucket %s at key %s", legacyBucketName, nonPrefixedKey)
        }
    }
    
    if iter.Error() != nil {
        log.Errorf("An error occurred while scanning through the legacy database: %v")
    }
    
    iter.Release()
    
    return nil
}

func DecodeLegacySiblingSet(data []byte) (*SiblingSet, error) {
    var lss legacySiblingSet
    
    err := json.Unmarshal(data, &lss)
    
    if err != nil {
        return nil, err
    }
    
    return lss.ToSiblingSet(), nil
}

type legacySiblingSet []legacySibling

func (lss *legacySiblingSet) ToSiblingSet() *SiblingSet {
    var siblings map[*Sibling]bool = make(map[*Sibling]bool, len(*lss))
    
    for _, ls := range *lss {
        siblings[ls.ToSibling()] = true
    }
    
    return NewSiblingSet(siblings)
}

type legacySibling struct {
    Value *string `json:"value"`
    Clock legacyDVV `json:"clock"`
    CreationTime uint64 `json:"creationTime"`
}

func (ls *legacySibling) ToSibling() *Sibling {
    var value []byte
    
    if ls.Value != nil {
        value = []byte(*ls.Value)
    }
    
    return NewSibling(ls.Clock.ToDVV(), value, ls.CreationTime)
}

type legacyDVV struct {
    Dot legacyDot `json:"dot"`
    Context []legacyDot `json:"context"`
}

func (ldvv *legacyDVV) ToDVV() *DVV {
    var context map[string]uint64 = make(map[string]uint64, len(ldvv.Context))
    
    for _, ld := range ldvv.Context {
        context[ld.node] = ld.count
    }
    
    return NewDVV(ldvv.Dot.ToDot(), context)
}

type legacyDot struct {
    node string
    count uint64
}

func (ld *legacyDot) ToDot() *Dot {
    return NewDot(ld.node, ld.count)
}

func (ld *legacyDot) MarshalJSON() ([]byte, error) {
    var a [2]interface{ }
    
    a[0] = ld.node
    a[1] = ld.count
    
    return json.Marshal(a)
}

func (ld *legacyDot) UnmarshalJSON(data []byte) error {
    var a [2]json.RawMessage
    
    err := json.Unmarshal(data, &a)
    
    if err != nil {
        return err
    }
    
    err = json.Unmarshal(a[0], &ld.node)
    
    if err != nil {
        return err
    }
    
    err = json.Unmarshal(a[1], &ld.count)
    
    if err != nil {
        return err
    }
    
    return nil
}
