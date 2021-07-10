package site_test
//
 // Copyright (c) 2019 ARM Limited.
 //
 // SPDX-License-Identifier: MIT
 //
 // Permission is hereby granted, free of charge, to any person obtaining a copy
 // of this software and associated documentation files (the "Software"), to
 // deal in the Software without restriction, including without limitation the
 // rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 // sell copies of the Software, and to permit persons to whom the Software is
 // furnished to do so, subject to the following conditions:
 //
 // The above copyright notice and this permission notice shall be included in all
 // copies or substantial portions of the Software.
 //
 // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 // SOFTWARE.
 //


import (
    . "github.com/PelionIoT/devicedb/site"
    . "github.com/PelionIoT/devicedb/storage"
    . "github.com/PelionIoT/devicedb/util"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("SiteFactory", func() {
    var storageDriver StorageDriver

    BeforeEach(func() {
        storageDriver = NewLevelDBStorageDriver("/tmp/testraftstore-" + RandomString(), nil)
        storageDriver.Open()
    })

    AfterEach(func() {
        storageDriver.Close()
    })

    Describe("RelaySiteFactory", func() {
        Describe("#CreateSite", func() {
            Specify("Should return a RelaySiteReplica Site", func() {
                relaySiteFactory := &RelaySiteFactory{
                    MerkleDepth: 4,
                    StorageDriver: storageDriver,
                    RelayID: "WWRL000000",
                }

                site := relaySiteFactory.CreateSite("site1")

                _, ok := site.(*RelaySiteReplica)
                Expect(ok).Should(BeTrue())
                Expect(len(site.Buckets().All())).Should(Equal(4))
                Expect(site.Buckets().Get("default")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("cloud")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("lww")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("local")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("default").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("cloud").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("lww").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("local").MerkleTree().Depth()).Should(Equal(uint8(1)))
            })
        })
    })

    Describe("CloudSiteFactory", func() {
        Describe("#CreateSite", func() {
            Specify("Should return a CloudSiteReplica Site", func() {
                cloudSiteFactory := &CloudSiteFactory{
                    MerkleDepth: 4,
                    StorageDriver: storageDriver,
                    NodeID: "Cloud-1",
                }

                site := cloudSiteFactory.CreateSite("site1")

                _, ok := site.(*CloudSiteReplica)
                Expect(ok).Should(BeTrue())
                Expect(len(site.Buckets().All())).Should(Equal(4))
                Expect(site.Buckets().Get("default")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("cloud")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("lww")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("local")).Should(Not(BeNil()))
                Expect(site.Buckets().Get("default").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("cloud").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("lww").MerkleTree().Depth()).Should(Equal(uint8(4)))
                Expect(site.Buckets().Get("local").MerkleTree().Depth()).Should(Equal(uint8(1)))
            })
        })
    })
})
