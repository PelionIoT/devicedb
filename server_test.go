package devicedb_test

import (
    "fmt"
    "time"
    "net/http"
    "bytes"
    "encoding/json"
    "bufio"
    
	. "devicedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
    var client *http.Client
    var server *Server
    stop := make(chan int)
    
    BeforeEach(func() {
        client = &http.Client{ Transport: &http.Transport{ DisableKeepAlives: true } }
        server, _ = NewServer("/tmp/testdb-" + randomString())
        
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
        
    url := func(u string, server *Server) string {
        return "http://localhost:" + fmt.Sprintf("%d", server.Port()) + u
    }
    
    buffer := func(j string) *bytes.Buffer {
        return bytes.NewBuffer([]byte(j))
    }
    
    Describe("POST /{bucket}/values", func() {
        Context("The values being queried are empty", func() {
            It("should return nil for every key", func() {
                resp, err := client.Post(url("/default/values", server), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
                
                Expect(err).Should(BeNil())
            
                siblingSets := make([]*SiblingSet, 0)
                decoder := json.NewDecoder(resp.Body)
                err = decoder.Decode(&siblingSets)
                
                Expect(err).Should(BeNil())
                Expect(siblingSets).Should(Equal([]*SiblingSet{ nil, nil, nil }))
                Expect(resp.StatusCode).Should(Equal(http.StatusOK))
            })
        })
        
        It("Should return 404 with EInvalidBucket in the body if the bucket specified is invalid", func() {
            resp, err := client.Post(url("/invalidbucket/values", server), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidBucket))
            Expect(resp.StatusCode).Should(Equal(http.StatusNotFound))
        })
        
        It("Should return 400 with EInvalidKey in the body if a null key value was specified", func() {
            resp, err := client.Post(url("/default/values", server), "application/json", buffer(`[ null, "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
        
        It("Should return 400 with EInvalidKey in the body if an empty key was specified", func() {
            resp, err := client.Post(url("/default/values", server), "application/json", buffer(`[ "", "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
        
        It("Should return 400 with EInvalidKey in the body if the request body is not an array", func() {
            resp, err := client.Post(url("/default/values", server), "application/json", buffer(`null`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
            
            resp, err = http.Post(url("/default/values", server), "application/json", buffer(`{ "0": "key1" }`))
                
            Expect(err).Should(BeNil())
            
            decoder = json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
    })
    
    Describe("POST /{bucket}/matches", func() {
        Context("The values being queried are empty", func() {
            It("should return nil for every key", func() {
                
                updateBatch := NewUpdateBatch()
                updateBatch.Put([]byte("key1"), []byte("value1"), NewDVV(NewDot("", 0), map[string]uint64{ }))
                updateBatch.Put([]byte("key2"), []byte("value2"), NewDVV(NewDot("", 0), map[string]uint64{ }))
                updateBatch.Put([]byte("key3"), []byte("value3"), NewDVV(NewDot("", 0), map[string]uint64{ }))
                jsonBytes, _ := updateBatch.ToJSON()
            
                resp, err := client.Post(url("/default/batch", server), "application/json", bytes.NewBuffer(jsonBytes))
                
                Expect(err).Should(BeNil())
                resp.Body.Close()
                
                resp, err = client.Post(url("/default/matches", server), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
                defer resp.Body.Close()
                
                values := [][]string{
                    []string{ "key1", "value1", },
                    []string{ "key2", "value2", },
                    []string{ "key3", "value3", },
                }
                
                scanner := bufio.NewScanner(resp.Body)
                
                for scanner.Scan() {
                    l := values[0]
                    values = values[1:]
                    
                    Expect(scanner.Text()).Should(Equal(l[0]))
                    Expect(scanner.Scan()).Should(BeTrue())
                    Expect(scanner.Text()).Should(Equal(l[0]))
                    Expect(scanner.Scan()).Should(BeTrue())
                    
                    var siblingSet SiblingSet
                
                    fmt.Println(scanner.Text())
                    decoder := json.NewDecoder(bytes.NewBuffer(scanner.Bytes()))
                    err = decoder.Decode(&siblingSet)
                    Expect(err).Should(BeNil())
                    Expect(siblingSet.Value()).Should(Equal([]byte(l[1])))
                }
                
                Expect(scanner.Err()).Should(BeNil())
            })
        })
        
        It("Should return 404 with EInvalidBucket in the body if the bucket specified is invalid", func() {
            resp, err := client.Post(url("/invalidbucket/matches", server), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidBucket))
            Expect(resp.StatusCode).Should(Equal(http.StatusNotFound))
        })
        
        It("Should return 400 with EInvalidKey in the body if a null key value was specified", func() {
            resp, err := client.Post(url("/default/matches", server), "application/json", buffer(`[ null, "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
        
        It("Should return 400 with EInvalidKey in the body if an empty key was specified", func() {
            resp, err := client.Post(url("/default/matches", server), "application/json", buffer(`[ "", "key2", "key3" ]`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
        
        It("Should return 400 with EInvalidKey in the body if the request body is not an array", func() {
            resp, err := client.Post(url("/default/matches", server), "application/json", buffer(`null`))
                
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            
            var dberr DBerror
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
            
            resp, err = http.Post(url("/default/values", server), "application/json", buffer(`{ "0": "key1" }`))
                
            Expect(err).Should(BeNil())
            
            decoder = json.NewDecoder(resp.Body)
            err = decoder.Decode(&dberr)
            
            Expect(err).Should(BeNil())
            Expect(dberr).Should(Equal(EInvalidKey))
            Expect(resp.StatusCode).Should(Equal(http.StatusBadRequest))
        })
    })
    
    Describe("POST /{bucket}/batch", func() {
        It("should put the values specified", func() {
            updateBatch := NewUpdateBatch()
            updateBatch.Put([]byte("key1"), []byte("value1"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            updateBatch.Put([]byte("key2"), []byte("value2"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            updateBatch.Put([]byte("key3"), []byte("value3"), NewDVV(NewDot("", 0), map[string]uint64{ }))
            jsonBytes, _ := updateBatch.ToJSON()
        
            resp, err := client.Post(url("/default/batch", server), "application/json", bytes.NewBuffer(jsonBytes))
            
            Expect(err).Should(BeNil())
            defer resp.Body.Close()
            Expect(resp.StatusCode).Should(Equal(http.StatusOK))
            
            resp, err = client.Post(url("/default/values", server), "application/json", buffer(`[ "key1", "key2", "key3" ]`))
            
            Expect(err).Should(BeNil())
        
            siblingSets := make([]*SiblingSet, 0)
            decoder := json.NewDecoder(resp.Body)
            err = decoder.Decode(&siblingSets)
            
            Expect(err).Should(BeNil())
            Expect(siblingSets[0].Value()).Should(Equal([]byte("value1")))
            Expect(siblingSets[1].Value()).Should(Equal([]byte("value2")))
            Expect(siblingSets[2].Value()).Should(Equal([]byte("value3")))
            Expect(resp.StatusCode).Should(Equal(http.StatusOK))
        })
    })
})
