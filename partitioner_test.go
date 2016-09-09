package devicedb_test

import (
    "errors"
    "math"
    "strconv"
    "sort"
    
	. "devicedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Partitioner", func() {
    Describe("RingState", func() {
        Describe("#SortedTokens", func() {
            It("should return a sorted list of tokens along the ring", func() {
                ringState := NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeA": TokenSet{ 0, map[uint64]bool{ 23: true } },
                    "nodeB": TokenSet{ 0, map[uint64]bool{ 22: true } },
                    "nodeC": TokenSet{ 0, map[uint64]bool{ 28: true } },
                    "nodeD": TokenSet{ 0, map[uint64]bool{ 28: true, 1: true } },
                })
                
                Expect(ringState.SortedTokens()).Should(Equal([]Token{
                    Token{ "nodeD", 1 },
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                }))
            })
        })
        
        Describe("#TokenRingIterator", func() {
            It("should traverse all tokens in the ring in order starting at the token that comes directly after the start point", func() {
                ringState := NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeA": TokenSet{ 0, map[uint64]bool{ 23: true } },
                    "nodeB": TokenSet{ 0, map[uint64]bool{ 22: true } },
                    "nodeC": TokenSet{ 0, map[uint64]bool{ 28: true } },
                    "nodeD": TokenSet{ 0, map[uint64]bool{ 28: true, 1: true } },
                })
                
                traverseAll := func(iter *TokenRingIterator) []Token {
                    all := make([]Token, 0)
                    
                    for iter.HasNextToken() {
                        all = append(all, *iter.NextToken())
                    }
                    
                    return all
                }
                
                ringIterator0 := ringState.TokenRingIterator(0)
                ringIterator1 := ringState.TokenRingIterator(19)
                ringIterator2 := ringState.TokenRingIterator(22)
                ringIterator3 := ringState.TokenRingIterator(28)
                ringIterator4 := ringState.TokenRingIterator(1024)
                
                Expect(traverseAll(ringIterator0)).Should(Equal([]Token{
                    Token{ "nodeD", 1 },
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                }))
                
                Expect(traverseAll(ringIterator1)).Should(Equal([]Token{
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                    Token{ "nodeD", 1 },
                }))
                
                Expect(traverseAll(ringIterator2)).Should(Equal([]Token{
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                    Token{ "nodeD", 1 },
                }))
                
                Expect(traverseAll(ringIterator3)).Should(Equal([]Token{
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                    Token{ "nodeD", 1 },
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                }))
                
                Expect(traverseAll(ringIterator4)).Should(Equal([]Token{
                    Token{ "nodeD", 1 },
                    Token{ "nodeB", 22 },
                    Token{ "nodeA", 23 },
                    Token{ "nodeC", 28 },
                    Token{ "nodeD", 28 },
                }))
            })
        })
    })
    
    Describe("HashRing", func() {
        Describe("#NewHashRing", func() {
            It("should return an error if the token count is 0", func() {
                _, err := NewHashRing("nodeA", 0, nil)
                
                Expect(err).Should(MatchError(errors.New("tokens must be positive")))
            })
        })
        
        Describe("#GenerateTokens", func() {
            It("should produce the number of tokens specified in the hash ring constructor", func() {
                for i := 1; i < 256; i += 1 {
                    hashRing, _ := NewHashRing("nodeA", uint8(i), nil)
                    Expect(len(hashRing.GenerateTokens())).Should(Equal(i))
                }
            })
            
            It("adjacent tokens should be at least floor(tokenCount/4) apart", func() {
                for i := 1; i < 256; i += 1 {
                    hashRing, _ := NewHashRing("nodeA", uint8(i), nil)
                    tokens := hashRing.GenerateTokens()
                    tokenSeparation := (hashRing.HashMax() / uint64(i)) % math.MaxUint64
                    
                    for j := 0; j < len(tokens); j += 1 {
                        d := tokens[(j + 1) % len(tokens)] - tokens[j]
                
                        // between 1/4 and 7/4 tokenSeparation apart
                        Expect(d).Should(BeNumerically(">=", tokenSeparation / 4))
                        Expect(d).Should(BeNumerically("<=", 7 * (tokenSeparation / 4)))
                    }
                }
            })
        })
        
        Describe("#Update", func() {
            It("should merge in token assignment information from another node if we did not previously know about that node", func() {
                hashRing, _ := NewHashRing("nodeA", uint8(3), nil)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ }))
                
                ringState := NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeB": TokenSet{ 1, map[uint64]bool{ 89: true, 77: true } },
                })
                
                hashRing.Update(ringState)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ 
                    "nodeB": 1,
                }))
            })
            
            It("should only overwrite the token assignment information that we have for another node if its version is more up to date", func() {
                hashRing, _ := NewHashRing("nodeA", uint8(3), nil)
                
                Expect(hashRing.RingState().Tokens("nodeB")).Should(BeNil())
                    
                ringState := NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeB": TokenSet{ 1, map[uint64]bool{ 89: true, 77: true } },
                    "nodeC": TokenSet{ 1, map[uint64]bool{ 1: true, 2: true } },
                })
                
                hashRing.Update(ringState)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ 
                    "nodeB": 1,
                    "nodeC": 1,
                }))
                
                Expect(hashRing.RingState().Tokens("nodeA")).Should(BeNil())
                Expect(hashRing.RingState().Tokens("nodeB")).Should(Equal(map[uint64]bool{ 89: true, 77: true }))
                Expect(hashRing.RingState().Tokens("nodeC")).Should(Equal(map[uint64]bool{ 1: true, 2: true }))
                
                ringState = NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeB": TokenSet{ 2, map[uint64]bool{ 100: true } },
                    "nodeC": TokenSet{ 1, map[uint64]bool{ 1000: true, 2000: true } },
                })
                
                hashRing.Update(ringState)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ 
                    "nodeB": 2,
                    "nodeC": 1,
                }))
                
                Expect(hashRing.RingState().Tokens("nodeA")).Should(BeNil())
                Expect(hashRing.RingState().Tokens("nodeB")).Should(Equal(map[uint64]bool{ 100: true }))
                Expect(hashRing.RingState().Tokens("nodeC")).Should(Equal(map[uint64]bool{ 1: true, 2: true }))
            })
            
            It("should only update the version number if it is this node id", func() {
                hashRing, _ := NewHashRing("nodeA", uint8(3), nil)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ }))
                Expect(hashRing.RingState().Tokens("nodeB")).Should(BeNil())
                    
                ringState := NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeA": TokenSet{ 1, map[uint64]bool{ 23: true, 323: true, 32322: true } },
                })
                
                hashRing.Update(ringState)
                
                ringState = NewRingState()
                
                ringState.SetTokens(map[string]TokenSet{
                    "nodeA": TokenSet{ 5, map[uint64]bool{ 555: true, 666: true } },
                    "nodeB": TokenSet{ 1, map[uint64]bool{ 89: true, 77: true } },
                    "nodeC": TokenSet{ 1, map[uint64]bool{ 1: true, 2: true } },
                })
                
                hashRing.Update(ringState)
                
                Expect(hashRing.RingState().Version()).Should(Equal(map[string]uint64{ 
                    "nodeA": 6,
                    "nodeB": 1,
                    "nodeC": 1,
                }))
                
                Expect(len(hashRing.RingState().Tokens("nodeA"))).Should(Equal(3))
                Expect(hashRing.RingState().Tokens("nodeB")).Should(Equal(map[uint64]bool{ 89: true, 77: true }))
                Expect(hashRing.RingState().Tokens("nodeC")).Should(Equal(map[uint64]bool{ 1: true, 2: true }))
            })
        })
        
        Describe("#PreferenceList", func() {
            type RangeFenceposts struct {
                min uint64
                mid uint64
                max uint64
            }
            
            generateDivisionFenceposts := func(min, max, divisions uint64) []RangeFenceposts {
                keyMappings := make([]RangeFenceposts, 0)
                divisionSize := (max - min) / divisions
                currentMin := min
                
                for i := 0; i < int(divisions); i += 1 { 
                    rangeMin := currentMin
                    rangeMax := min + divisionSize - 1
                    rangeMid := rangeMin + divisionSize / 2
                    
                    currentMin += divisionSize
                    
                    keyMappings = append(keyMappings, RangeFenceposts{ rangeMin, rangeMid, rangeMax })
                }
                
                return keyMappings
            }
            
            getDivisionIndexFromToken := func(token, maxHash, divisions uint64) uint64 {
                divisionSize := (maxHash / divisions) + 1
                
                return token / divisionSize
            }
            
            getDivisionStartFromToken := func(token, maxHash, divisions uint64) uint64 {
                divisionSize := (maxHash / divisions) + 1
                
                return getDivisionIndexFromToken(token, maxHash, divisions) * divisionSize
            }
            
            getPreferenceList := func(token, maxHash, divisions uint64, ringStructure []Token, replicationFactor uint64) map[string]bool {
                tokenDivisionStart := getDivisionStartFromToken(token, maxHash, divisions)
                first := 0
                
                for first < len(ringStructure) {
                    if tokenDivisionStart < ringStructure[first].Token {
                        break
                    } else if tokenDivisionStart > ringStructure[first].Token {
                        first += 1
                    } else {
                        break
                    }
                }
                
                if first == len(ringStructure) {
                    first = 0
                }
                
                preferenceList := map[string]bool{ }
                currentIndex := first
                
                for i := 0; i < len(ringStructure) && len(preferenceList) < int(replicationFactor); i += 1 {
                    nextToken := ringStructure[currentIndex]
                    
                    preferenceList[nextToken.NodeID] = true
                    
                    currentIndex = (currentIndex + 1) % len(ringStructure)
                }
                
                return preferenceList
            }
            
            It("if the number of replicas is equal to the replication factor, all calls should return a list with all replicas", func() {
                dummy, _ := NewHashRing("dummy", uint8(3), nil)
                nodes := []*HashRing{ }
                divisionFenceposts := generateDivisionFenceposts(dummy.HashMin(), dummy.HashMax(), dummy.Divisions())
                tokenStrategyMappings := map[string]uint64{ }
                
                for i := 0; i < len(divisionFenceposts); i += 1 {
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].min
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].mid
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].max
                }
                
                for i := 0; i < dummy.ReplicationFactor(); i += 1 {
                    hashRing, _ := NewHashRing("node" + strconv.Itoa(i), uint8(3), func(key []byte) uint64 {
                        if hash, ok := tokenStrategyMappings[string(key)]; ok {
                            return hash
                        }
                        
                        return 0
                    })
                    
                    nodes = append(nodes, hashRing)
                }
                
                tokens := [][]uint64{ }
                tokensUpdate := map[string]TokenSet{ }
                ringState := NewRingState()
                
                for _, node := range nodes {
                    initTokens := node.GenerateTokens()
                    initTokensMap := map[uint64]bool{ }
                    
                    for _, t := range initTokens {
                        initTokensMap[t] = true
                    }
                    
                    tokens = append(tokens, initTokens)
                    
                    tokensUpdate[node.NodeID()] = TokenSet{ 1, initTokensMap }
                }
                
                ringState.SetTokens(tokensUpdate)
                
                for _, node := range nodes {
                    node.Update(ringState)
                }
                
                ringStructure := []Token{ }
                
                for i := 0; i < len(nodes); i += 1 {
                    nodeTokenList := tokens[i]
                    
                    for j := 0; j < len(nodeTokenList); j += 1 {
                        ringStructure = append(ringStructure, Token{ "node" + strconv.Itoa(i), nodeTokenList[j] })
                    }
                }
                
                sort.Sort(TokenList(ringStructure))
                
                for key, token := range tokenStrategyMappings {
                    actualPreferenceList := nodes[0].PreferenceList([]byte(key))
                    expectedPreferenceList := getPreferenceList(token, nodes[0].HashMax(), nodes[0].Divisions(), ringStructure, uint64(nodes[0].ReplicationFactor()))
                    
                    Expect(actualPreferenceList).Should(Equal(expectedPreferenceList))
                }
            })
            
            It("if number of replicas is greater than the replication factor, all calls should return a list of replicas whose length is the replication factor", func() {
                dummy, _ := NewHashRing("dummy", uint8(3), nil)
                nodes := []*HashRing{ }
                divisionFenceposts := generateDivisionFenceposts(dummy.HashMin(), dummy.HashMax(), dummy.Divisions())
                tokenStrategyMappings := map[string]uint64{ }
                
                for i := 0; i < len(divisionFenceposts); i += 1 {
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].min
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].mid
                    tokenStrategyMappings["key" + strconv.Itoa(len(tokenStrategyMappings))] = divisionFenceposts[i].max
                }
                
                for i := 0; i < dummy.ReplicationFactor() + 1; i += 1 {
                    hashRing, _ := NewHashRing("node" + strconv.Itoa(i), uint8(3), func(key []byte) uint64 {
                        if hash, ok := tokenStrategyMappings[string(key)]; ok {
                            return hash
                        }
                        
                        return 0
                    })
                    
                    nodes = append(nodes, hashRing)
                }
                
                tokens := [][]uint64{ }
                tokensUpdate := map[string]TokenSet{ }
                ringState := NewRingState()
                
                for _, node := range nodes {
                    initTokens := node.GenerateTokens()
                    initTokensMap := map[uint64]bool{ }
                    
                    for _, t := range initTokens {
                        initTokensMap[t] = true
                    }
                    
                    tokens = append(tokens, initTokens)
                    
                    tokensUpdate[node.NodeID()] = TokenSet{ 1, initTokensMap }
                }
                
                ringState.SetTokens(tokensUpdate)
                
                for _, node := range nodes {
                    node.Update(ringState)
                }
                
                ringStructure := []Token{ }
                
                for i := 0; i < len(nodes); i += 1 {
                    nodeTokenList := tokens[i]
                    
                    for j := 0; j < len(nodeTokenList); j += 1 {
                        ringStructure = append(ringStructure, Token{ "node" + strconv.Itoa(i), nodeTokenList[j] })
                    }
                }
                
                sort.Sort(TokenList(ringStructure))
                
                for key, token := range tokenStrategyMappings {
                    actualPreferenceList := nodes[0].PreferenceList([]byte(key))
                    expectedPreferenceList := getPreferenceList(token, nodes[0].HashMax(), nodes[0].Divisions(), ringStructure, uint64(nodes[0].ReplicationFactor()))
                    
                    Expect(actualPreferenceList).Should(Equal(expectedPreferenceList))
                }
            })
        })
    })
})
