package routes

import (
    . "devicedb/data"
    . "devicedb/transport"
)

type InternalEntry struct {
    Prefix string
    Key string
    Siblings *SiblingSet
}

func (entry *InternalEntry) ToAPIEntry() *APIEntry {
    var transportSiblingSet TransportSiblingSet

    transportSiblingSet.FromSiblingSet(entry.Siblings)

    return &APIEntry{
        Prefix: entry.Prefix,
        Key: entry.Key,
        Context: transportSiblingSet.Context,
        Siblings: transportSiblingSet.Siblings,
    }
}

type APIEntry struct {
    Prefix string `json:"prefix"`
    Key string `json:"key"`
    Context string `json:"context"`
    Siblings []string `json:"siblings"`
}

type BatchResult struct {
    // Number of replicas that the batch was successfully applied to
    NApplied uint64 `json:"nApplied"`
    // Number of replicas in the replica set for this site
    Replicas uint64 `json:"replicas"`
}

type RelaySettingsPatch struct {
    Site string `json:"site"`
}