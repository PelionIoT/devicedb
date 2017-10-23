package routes

import (
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/transport"
)

type ClusterOverview struct {
    Nodes []NodeConfig
    ClusterSettings ClusterSettings
    PartitionDistribution [][]uint64
    TokenAssignments []uint64
}

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
    // Was write quorum achieved
    Quorum bool
    Patch map[string]*SiblingSet `json:"patch"`
}

type RelaySettingsPatch struct {
    Site string `json:"site"`
}