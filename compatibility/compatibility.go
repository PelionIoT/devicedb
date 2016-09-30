// This file provides utility functions that make backwards compatibility with original DeviceDB (written in NodeJS)
// easier
package compatibility

import (
    "devicedb/dbobject"
    "encoding/json"
    "sort"
)

func SiblingToNormalizedJSON(sibling *dbobject.Sibling) string {
    return valueToJSON(sibling.Value()) + dotToJSON(sibling.Clock().Dot()) + contextToJSON(sibling.Clock().Context())
}

func valueToJSON(value []byte) string {
    if value == nil {
        return "null"
    }
    
    j, _ := json.Marshal(string(value))
    
    return string(j)
}

func dotToJSON(dot dbobject.Dot) string {
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

func HashSiblingSet(key string, siblingSet *dbobject.SiblingSet) dbobject.Hash {
    siblingsJSON := make([]string, 0, siblingSet.Size())
    
    for sibling := range siblingSet.Iter() {
        siblingsJSON = append(siblingsJSON, SiblingToNormalizedJSON(sibling))
    }
    
    sort.Strings(siblingsJSON)
    
    j, _ := json.Marshal(siblingsJSON)
    
    return dbobject.NewHash([]byte(key + string(j)))
}
