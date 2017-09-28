package clusterio

import (
    . "devicedb/data"
)

type ReadMerger struct {
    keyVersions map[string]map[uint64]*SiblingSet
    mergedKeys map[string]*SiblingSet
}

func NewReadMerger() *ReadMerger {
    return &ReadMerger{
        keyVersions: make(map[string]map[uint64]*SiblingSet),
        mergedKeys: make(map[string]*SiblingSet),
    }
}

func (readMerger *ReadMerger) InsertKeyReplica(nodeID uint64, key string, siblingSet *SiblingSet) {
    if _, ok := readMerger.keyVersions[key]; !ok {
        readMerger.keyVersions[key] = make(map[uint64]*SiblingSet)
    }

    if siblingSet == nil {
        siblingSet = NewSiblingSet(map[*Sibling]bool{ })
    }

    readMerger.keyVersions[key][nodeID] = siblingSet

    if readMerger.mergedKeys[key] == nil {
        readMerger.mergedKeys[key] = siblingSet
    } else {
        readMerger.mergedKeys[key] = readMerger.mergedKeys[key].Sync(siblingSet)
    }
}

func (readMerger *ReadMerger) Get(key string) *SiblingSet {
    if readMerger.mergedKeys[key] == nil {
        return nil
    }

    if readMerger.mergedKeys[key].Size() == 0 {
        return nil
    }
    
    return readMerger.mergedKeys[key]
}

func (readMerger *ReadMerger) Patch(nodeID uint64) map[string]*SiblingSet {
    var patch map[string]*SiblingSet = make(map[string]*SiblingSet, len(readMerger.keyVersions))

    for key, versions := range readMerger.keyVersions {
        if version, ok := versions[nodeID]; ok {
            patch[key] = version.Diff(readMerger.mergedKeys[key])
        } else {
            patch[key] = readMerger.mergedKeys[key]
        }
    }

    return patch
}
