package devicedb_test

import (
	. "devicedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
    
    "fmt"
)

var _ = Describe("Compatibility", func() {
    Describe("HashSiblingSet", func() {
        It("Should work", func() {
            sibling1 := NewSibling(NewDVV(NewDot("r1", 1), map[string]uint64{ "r2": 5, "r3": 2 }), []byte("v1"), 0)
            sibling2 := NewSibling(NewDVV(NewDot("r1", 2), map[string]uint64{ "r2": 4, "r3": 3 }), []byte("v2"), 0)
            sibling3 := NewSibling(NewDVV(NewDot("r2", 6), map[string]uint64{ }), []byte("v3"), 0)
            sibling4 := NewSibling(NewDVV(NewDot("r1", 2), map[string]uint64{ "r2": 4, "r3": 44 }), []byte("v2"), 0)
            
            siblingSet1 := NewSiblingSet(map[*Sibling]bool{
                sibling1: true,
                sibling2: true,
                sibling3: true,
            })
            
            siblingSet2 := NewSiblingSet(map[*Sibling]bool{
                sibling1: true,
                sibling2: true,
            })
            
            siblingSet3 := NewSiblingSet(map[*Sibling]bool{
                sibling3: true,
                sibling1: true,
                sibling4: true,
            })
            
            h1 := HashSiblingSet("a.b.c", siblingSet1)
            h2 := HashSiblingSet("a.b.c", siblingSet2)
            h3 := HashSiblingSet("a.b.c", siblingSet3)
            
            r1 := fmt.Sprintf("0x%016x%016x", h1.High(), h1.Low())
            r2 := fmt.Sprintf("0x%016x%016x", h2.High(), h2.Low())
            r3 := fmt.Sprintf("0x%016x%016x", h3.High(), h3.Low())
            
            Expect(r1).Should(Equal("0xc021c4098675bd0ede160fd752fa2016"))
            Expect(r2).Should(Equal("0xc421f3b60f57006416b5deca38fc8ad4"))
            Expect(r3).Should(Equal("0x487124149c7b1aa8accfcd4574f46fcf"))
        })
    })
    
    Describe("DecodeLegacySiblingSet", func() {
        It("should turn a legacy json representation of a sibling set into a SiblingSet object", func() {
            legacySiblingSetJSON := `[{"value":"{\"groups\":{\"A/B/C\":true},\"type\":\"ResourceType1\",\"interfaces\":[\"InterfaceType1\",\"InterfaceType2\"]}","clock":{"dot":["00000000000000000000000000000000",2],"context":[["00000000000000000000000000000000",1]]},"creationTime":1469218230589}]`
            sibling1 := NewSibling(NewDVV(NewDot("00000000000000000000000000000000", 2), map[string]uint64{ "00000000000000000000000000000000": 1 }), []byte("{\"groups\":{\"A/B/C\":true},\"type\":\"ResourceType1\",\"interfaces\":[\"InterfaceType1\",\"InterfaceType2\"]}"), 1469218230589)
            
            expectedDecodedSet := NewSiblingSet(map[*Sibling]bool{
                sibling1: true,
            })
        
            ss, err := DecodeLegacySiblingSet([]byte(legacySiblingSetJSON))
            
            Expect(err).Should(BeNil())
            Expect(ss.Size()).Should(Equal(expectedDecodedSet.Size()))
            
            for sibling := range ss.Iter() {
                Expect(sibling).Should(Equal(sibling1))
            }
        })
    })
})
