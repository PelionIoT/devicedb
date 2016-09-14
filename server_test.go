package devicedb_test

import (
    "fmt"
    "time"
    "net/http"
    "bytes"
    "encoding/json"
    
	. "devicedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
    var server *Server
    stop := make(chan int)
    
    BeforeEach(func() {
        server, _ = NewServer()
        
        go func() {
            server.Start()
            stop <- 1
        }()
        
        time.Sleep(time.Millisecond * 100)
    })
    
    AfterEach(func() {
        server.Stop()
        <-stop
    })
    
    url := func(u string) string {
        return "http://localhost:" + fmt.Sprintf("%d", server.Port()) + u
    }
    
    buffer := func(j string) *bytes.Buffer {
        return bytes.NewBuffer([]byte(j))
    }
    
    Describe("POST /{bucket}/values", func() {
        Context("The values being queried are empty", func() {
            It("should return nil for every key", func() {
                resp, err := http.Post(url("/default/values"), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
                
                Expect(err).Should(BeNil())
                
                siblingSets := make([]*SiblingSet, 0)
                decoder := json.NewDecoder(resp.Body)
                err = decoder.Decode(&siblingSets)
                
                Expect(err).Should(BeNil())
                Expect(siblingSets).Should(Equal([]*SiblingSet{ nil, nil, nil }))
            })
        })
    })
})
