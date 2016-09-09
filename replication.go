package devicedb

// get:         key -> [ nodes ]
// get matches: key -> node
// batch:       key -> [ nodes ]

type DataCenter struct {
    id string
    hashRing *HashRing
}

func NewDataCenter(id string, nodes map[string]uint64, hashStrategy func([]byte) uint64) *DataCenter {
    hashRing, _ := NewHashRing("", 1, hashStrategy)
    ringState := NewRingState()
    tokens := map[string]TokenSet{ }
    
    for nodeID, nodeToken := range nodes {
        tokenSet := map[uint64]bool{ }
        tokenSet[nodeToken] = true
        tokens[nodeID] = TokenSet{ 1, tokenSet }
    }
    
    ringState.SetTokens(tokens)
    hashRing.Update(ringState)
    
    return &DataCenter{ id, hashRing }
}

func (dataCenter *DataCenter) ID() string {
    return dataCenter.id
}

func (dataCenter *DataCenter) HashRing() *HashRing {
    return dataCenter.hashRing
}

type NetworkReplicationStrategy struct {
    thisNodeID string
    thisDataCenterID string
    dataCenters map[string]*DataCenter
}

func NewReplicationStrategy(thisNodeID string) *NetworkReplicationStrategy {
    return &NetworkReplicationStrategy{ thisNodeID, "", map[string]*DataCenter{ } }
}

func (replicationStrategy *NetworkReplicationStrategy) AddDataCenter(dataCenter *DataCenter) {
    if dataCenter.HashRing().RingState().Tokens(replicationStrategy.thisNodeID) != nil {
        replicationStrategy.thisDataCenterID = dataCenter.ID()
    }
    
    replicationStrategy.dataCenters[dataCenter.ID()] = dataCenter
}

func (replicationStrategy *NetworkReplicationStrategy) Replicas(key []byte) []string {
    replicas := make([]string, 0)

    if thisDataCenter, ok := replicationStrategy.dataCenters[replicationStrategy.thisDataCenterID]; ok {
        preferenceList := thisDataCenter.HashRing().PreferenceList(key)
    
        if _, ok := preferenceList[replicationStrategy.thisNodeID]; ok {
            replicas = append(replicas, replicationStrategy.thisNodeID)
        }
        
        for nodeID, _ := range preferenceList {
            if nodeID == replicationStrategy.thisNodeID {
                continue
            }
            
            replicas = append(replicas, nodeID)
        }
    }
    
    for _, dataCenter := range replicationStrategy.dataCenters {
        if dataCenter.ID() == replicationStrategy.thisDataCenterID {
            continue
        }
        
        preferenceList := dataCenter.HashRing().PreferenceList(key)
        
        for nodeID, _ := range preferenceList {
            replicas = append(replicas, nodeID)
        }
    }
    
    return replicas
}

