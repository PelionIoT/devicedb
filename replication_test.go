package devicedb_test

import (
    "sort"
    
	. "devicedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Replication", func() {
    Describe("#Replicas", func() {
        It("should prioritize nodes from the local data center first and put the local node first", func() {
            hashStrategy := func(key []byte) uint64 {
                return 0
            }
            
            networkReplicationStrategy := NewReplicationStrategy("nodeB")
            
            networkReplicationStrategy.AddDataCenter(NewDataCenter("dc1", map[string]uint64{ 
                "nodeA": 0,
                "nodeB": 1,
                "nodeC": 2,
                "nodeD": 3,
            }, hashStrategy))
            
            networkReplicationStrategy.AddDataCenter(NewDataCenter("dc2", map[string]uint64{ 
                "nodeE": 0,
                "nodeF": 1,
                "nodeG": 2,
                "nodeH": 3,
            }, hashStrategy))
            
            preferenceList := networkReplicationStrategy.Replicas([]byte("key"))
        
            first := preferenceList[0:3]
            second := preferenceList[3:]
            
            Expect(first[0]).Should(Equal("nodeB"))
            
            sort.Strings(first)
            sort.Strings(second)
            
            Expect(first).Should(Equal([]string{ "nodeA", "nodeB", "nodeC" }))
            Expect(second).Should(Equal([]string{ "nodeE", "nodeF", "nodeG" }))
        })
        
        It("should not include the local node in the result set if it is not in the preference lists", func() {
            hashStrategy := func(key []byte) uint64 {
                return 0
            }
            
            networkReplicationStrategy := NewReplicationStrategy("nodeD")
            
            networkReplicationStrategy.AddDataCenter(NewDataCenter("dc1", map[string]uint64{ 
                "nodeA": 0,
                "nodeB": 1,
                "nodeC": 2,
                "nodeD": 3,
            }, hashStrategy))
            
            networkReplicationStrategy.AddDataCenter(NewDataCenter("dc2", map[string]uint64{ 
                "nodeE": 0,
                "nodeF": 1,
                "nodeG": 2,
                "nodeH": 3,
            }, hashStrategy))
            
            preferenceList := networkReplicationStrategy.Replicas([]byte("key"))
        
            first := preferenceList[0:3]
            second := preferenceList[3:]
            
            sort.Strings(first)
            sort.Strings(second)
            
            Expect(first).Should(Equal([]string{ "nodeA", "nodeB", "nodeC" }))
            Expect(second).Should(Equal([]string{ "nodeE", "nodeF", "nodeG" }))
        })
    })
})
