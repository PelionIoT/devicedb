package halodb

import (
    "unsafe"
    "errors"
    "sort"
    "crypto/rand"
    mathRand "math/rand"
    "encoding/binary"
)

const (
    REPLICATION_FACTOR = 3
    HASH_RANGE_DIVISIONS = 8 // Indicates hash range of 2^8
    HASH_SIZE = 64
)

type RingState struct {
    tokenAssignments map[string]TokenSet
}

type TokenRingIterator struct {
    firstTokenIndex int
    currentOffset int
    sortedTokens []Token
}

type TokenSet struct {
    Version uint64
    Tokens map[uint64]bool
}

type Token struct {
    NodeID string
    Token uint64
}

type TokenList []Token

func (tokenList TokenList) Len() int {
    return len(tokenList)
}

func (tokenList TokenList) Swap(i, j int) {
    tokenList[i], tokenList[j] = tokenList[j], tokenList[i]
}

func (tokenList TokenList) Less(i, j int) bool {
    if tokenList[i].Token < tokenList[j].Token {
        return true
    } else if tokenList[i].Token == tokenList[j].Token {
        if tokenList[i].NodeID < tokenList[j].NodeID {
            return true
        }
    }
    
    return false
}

func NewRingState() *RingState {
    return &RingState{ make(map[string]TokenSet) }
}

func (ringState *RingState) SetTokens(tokens map[string]TokenSet) {
    ringState.tokenAssignments = tokens
}

func (ringState *RingState) Version() map[string]uint64 {
    version := map[string]uint64{ }
    
    for nodeID, tokenSet := range ringState.tokenAssignments {
        version[nodeID] = tokenSet.Version
    }
    
    return version
}

func (ringState *RingState) Tokens(nodeID string) map[uint64]bool {
    if _, ok := ringState.tokenAssignments[nodeID]; !ok {
        return nil
    }
    
    return ringState.tokenAssignments[nodeID].Tokens
}

func (ringState *RingState) SortedTokens() []Token {
    sortedTokens := make([]Token, 0)
    
    for nodeID, tokenSet := range ringState.tokenAssignments {
        for token, _ := range tokenSet.Tokens {
            sortedTokens = append(sortedTokens, Token{ nodeID, token })
        }
    }
    
    sort.Sort(TokenList(sortedTokens))
    
    return sortedTokens
}

func (ringState *RingState) TokenRingIterator(startToken uint64) *TokenRingIterator {
    return NewTokenRingIterator(startToken, ringState.SortedTokens())
}

func NewTokenRingIterator(startToken uint64, sortedTokens []Token) *TokenRingIterator {
    var tokenRingIterator TokenRingIterator
    
    for i := 0; i < len(sortedTokens); i += 1 {
        if startToken <= sortedTokens[i].Token {
            break
        }
        
        tokenRingIterator.firstTokenIndex = (i + 1) % len(sortedTokens)
    }
    
    tokenRingIterator.currentOffset = 0
    tokenRingIterator.sortedTokens = sortedTokens
    
    return &tokenRingIterator
}

func (tokenRingIterator *TokenRingIterator) HasNextToken() bool {
    return tokenRingIterator.currentOffset < len(tokenRingIterator.sortedTokens)
}

func (tokenRingIterator *TokenRingIterator) NextToken() *Token {
    if !tokenRingIterator.HasNextToken() {
        return nil
    }
    
    result := &tokenRingIterator.sortedTokens[(tokenRingIterator.firstTokenIndex + tokenRingIterator.currentOffset) % len(tokenRingIterator.sortedTokens)]
    
    tokenRingIterator.currentOffset += 1
    
    return result
}

type HashRing struct {
    nodeID string
    tokens uint8
    ringState RingState
    hashStrategy func([]byte) uint64
}

func NewHashRing(nodeID string, tokens uint8, hashStrategy func([]byte) uint64) (*HashRing, error) {
    if tokens == 0 {
        return nil, errors.New("tokens must be positive")
    }
    
    return &HashRing{ nodeID, tokens, *NewRingState(), hashStrategy }, nil
}

func (hashRing *HashRing) HashMin() uint64 {
    return 0
}

func (hashRing *HashRing) HashMax() uint64 {
    var max uint64 = 1

    return (max << HASH_SIZE) - 1
}

func (hashRing *HashRing) DivisionRange() uint64 {
    var divisionSize uint64 = 1
    
    return (divisionSize << (HASH_SIZE - HASH_RANGE_DIVISIONS))
}

func (hashRing *HashRing) Divisions() uint64 {
    return 1 << HASH_RANGE_DIVISIONS
}

func (hashRing *HashRing) ReplicationFactor() int {
    return REPLICATION_FACTOR
}

func (hashRing *HashRing) NodeID() string {
    return hashRing.nodeID
}

func (hashRing *HashRing) RingState() *RingState {
    return &hashRing.ringState
}

func (hashRing *HashRing) Update(ringState *RingState) {
    ringStatePatch := map[string]TokenSet{ }
    
    for nodeID, version := range ringState.Version() {
        if myVersion, ok := hashRing.RingState().Version()[nodeID]; !ok || myVersion < version {
            ringStatePatch[nodeID] = TokenSet{ version, ringState.Tokens(nodeID) }
        }
    }
    
    if _, ok := ringStatePatch[hashRing.nodeID]; ok {
        if hashRing.RingState().Tokens(hashRing.nodeID) != nil {
            ringStatePatch[hashRing.nodeID] = TokenSet{ ringStatePatch[hashRing.nodeID].Version, hashRing.RingState().Tokens(hashRing.nodeID) }
        }
    
        ringStatePatch[hashRing.nodeID] = TokenSet{ ringStatePatch[hashRing.nodeID].Version + 1, ringStatePatch[hashRing.nodeID].Tokens }
    }
    
    if len(ringStatePatch) > 0 {
        for nodeID, version := range hashRing.RingState().Version() {
            if _, ok := ringStatePatch[nodeID]; !ok {
                ringStatePatch[nodeID] = TokenSet{ version, hashRing.RingState().Tokens(nodeID) }
            }
        }
        
        hashRing.RingState().SetTokens(ringStatePatch)
    }
}

func (hashRing *HashRing) RandomToken() uint64 {
    tokenBytes := make([]byte, unsafe.Sizeof(hashRing.HashMax()))
    rand.Read(tokenBytes)
    
    return binary.BigEndian.Uint64(tokenBytes)
}

func (hashRing *HashRing) GenerateTokens() []uint64 {
    tokenSeparation := hashRing.HashMax() / uint64(hashRing.tokens)
    min := hashRing.RandomToken()
    newTokens := make([]uint64, 0, int(hashRing.tokens))
    
    for len(newTokens) < int(hashRing.tokens) {
        offset := (tokenSeparation / 4) + (uint64(mathRand.Int63()) % (3 * (tokenSeparation / 4)))
        token := (min + offset) % hashRing.HashMax()
        
        newTokens = append(newTokens, token)
        
        min = (min + tokenSeparation) % hashRing.HashMax()
    }
    
    return newTokens
}

func (hashRing *HashRing) PreferenceList(key []byte) map[string]bool {
    if key == nil {
        return map[string]bool{ }
    }
    
    preferenceList := make(map[string]bool)
    tokenRingIterator := hashRing.RingState().TokenRingIterator(hashRing.DivisionRange() * hashRing.PartitionNumber(key))
    
    for tokenRingIterator.HasNextToken() {
        preferenceList[tokenRingIterator.NextToken().NodeID] = true
        
        if len(preferenceList) == hashRing.ReplicationFactor() {
            break
        }
    }
    
    return preferenceList
}

func (hashRing *HashRing) PreferenceLists() []map[string]bool {
    sortedTokens := hashRing.RingState().SortedTokens()
    preferenceLists := make([]map[string]bool, hashRing.Divisions())
    currentMin := hashRing.HashMin()
    
    for i := uint64(0); i < hashRing.Divisions(); i += 1 {
        index := 0
        
        for index < len(sortedTokens) {
            if currentMin < sortedTokens[index].Token {
                break
            } else if currentMin > sortedTokens[index].Token {
                index += 1
            } else {
                break
            }
        }
        
        if index == len(sortedTokens) {
            index = 0
        }
        
        preferenceLists[i] = map[string]bool{ }
        
        for j := 0; j < len(sortedTokens) && len(preferenceLists[i]) < hashRing.ReplicationFactor(); j += 1 {
            preferenceLists[i][sortedTokens[index].NodeID] = true
            
            index = (index + 1) % len(sortedTokens)
        }
        
        currentMin += hashRing.DivisionRange()
    }
    
    return preferenceLists
}

func (hashRing *HashRing) PartitionNumber(key []byte) uint64 {
    return hashRing.Token(key) / hashRing.DivisionRange()
}

func (hashRing *HashRing) Token(key []byte) uint64 {
    if key == nil {
        return 0
    }

    if hashRing.hashStrategy == nil {
        return NewHash(key).High()
    }
    
    return hashRing.hashStrategy(key)
}

func (hashRing *HashRing) NodePartitions(nodeID string) []uint64 {
    partitions := make([]uint64, 0)
    preferenceLists := hashRing.PreferenceLists()
    
    for partition, preferenceList := range preferenceLists {
        if _, ok := preferenceList[nodeID]; ok {
            partitions = append(partitions, uint64(partition))
        }
    }
    
    return partitions
}

func (hashRing *HashRing) MyPartitions() []uint64 {
    return hashRing.NodePartitions(hashRing.nodeID)
}
