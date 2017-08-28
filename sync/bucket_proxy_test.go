package sync_test

import (
    . "devicedb/bucket"
    . "devicedb/data"
    . "devicedb/merkle"
    . "devicedb/cluster"
    //. "devicedb/raft"
    . "devicedb/site"
    . "devicedb/sync"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

type DummySitePool struct {
    sites map[string]Site
    released map[string]int
}

func (dummySitePool *DummySitePool) Acquire(siteID string) Site {
    return dummySitePool.sites[siteID]
}

func (dummySitePool *DummySitePool) Release(siteID string) {
    if dummySitePool.released == nil {
        dummySitePool.released = make(map[string]int)
    }

    dummySitePool.released[siteID] = dummySitePool.released[siteID] + 1
}

func (dummySitePool *DummySitePool) Add(siteID string) {
}

func (dummySitePool *DummySitePool) Remove(siteID string) {
}

type DummySite struct {
    bucketList *BucketList
}

func (dummySite *DummySite) Buckets() *BucketList {
    if dummySite == nil {
        return NewBucketList()
    }

    return dummySite.bucketList
}

type DummyBucket struct {
    name string
    merkleTree *MerkleTree
}

func (dummyBucket *DummyBucket) Name() string {
    return dummyBucket.name
}

func (dummyBucket *DummyBucket) ShouldReplicateOutgoing(peerID string) bool {
    return false
}

func (dummyBucket *DummyBucket) ShouldReplicateIncoming(peerID string) bool {
    return false
}

func (dummyBucket *DummyBucket) ShouldAcceptWrites(clientID string) bool {
    return false
}

func (dummyBucket *DummyBucket) ShouldAcceptReads(clientID string) bool {
    return false
}

func (dummyBucket *DummyBucket) RecordMetadata() error {
    return nil
}

func (dummyBucket *DummyBucket) RebuildMerkleLeafs() error {
    return nil
}

func (dummyBucket *DummyBucket) MerkleTree() *MerkleTree {
    return dummyBucket.merkleTree
}

func (dummyBucket *DummyBucket) GarbageCollect(tombstonePurgeAge uint64) error {
    return nil
}

func (dummyBucket *DummyBucket) Get(keys [][]byte) ([]*SiblingSet, error) {
    return nil, nil
}

func (dummyBucket *DummyBucket) GetMatches(keys [][]byte) (SiblingSetIterator, error) {
    return nil, nil
}

func (dummyBucket *DummyBucket) GetSyncChildren(nodeID uint32) (SiblingSetIterator, error) {
    return nil, nil
}

func (dummyBucket *DummyBucket) Forget(keys [][]byte) error {
    return nil
}

func (dummyBucket *DummyBucket) Batch(batch *UpdateBatch) (map[string]*SiblingSet, error) {
    return nil, nil
}

func (dummyBucket *DummyBucket) Merge(siblingSets map[string]*SiblingSet) error {
    return nil
}

var _ = Describe("BucketProxy", func() {
    Describe("RelayBucketProxyFactory", func() {
        Describe("#CreateBucketProxy", func() {
            Specify("If the specified bucket name is not valid it should return an ENoLocalBucket error", func() {
                relayBucketProxyFactory := &RelayBucketProxyFactory{
                    SitePool: &DummySitePool{
                        sites: map[string]Site{
                            "site1": &DummySite{ 
                                bucketList: NewBucketList(),
                            },
                        },
                    },
                }

                bucketProxy, err := relayBucketProxyFactory.CreateBucketProxy(1, "site1", "bucketName")

                Expect(bucketProxy).Should(BeNil())
                Expect(err).Should(Equal(ENoLocalBucket))
            })

            Specify("If the specified bucket name valid it should return a local bucket proxy for that bucket", func() {
                bucket := &DummyBucket{
                    name: "dummy",
                }

                bucketList := NewBucketList()
                bucketList.AddBucket(bucket)

                relayBucketProxyFactory := &RelayBucketProxyFactory{
                    SitePool: &DummySitePool{
                        sites: map[string]Site{
                            "site1": &DummySite{ 
                                bucketList: bucketList,
                            },
                        },
                    },
                }

                bucketProxy, err := relayBucketProxyFactory.CreateBucketProxy(1, "site1", "dummy")
                _, ok := bucketProxy.(*LocalBucketProxy)

                Expect(ok).Should(BeTrue())
                Expect(bucketProxy).Should(Not(BeNil()))
                Expect(err).Should(BeNil())
            })
        })
    })

    Describe("CloudBucketProxyFactory", func() {
        Describe("#CreateBucketProxy", func() {
            Context("when nodeID == the local node's ID", func() {
                Context("and the site has not been added to the pool", func() {
                    It("should return ENoLocalBucket", func() {
                        cloudBucketProxyFactory := &CloudBucketProxyFactory{
                            SitePool: &DummySitePool{
                                sites: map[string]Site{ },
                            },
                            ClusterController: &ClusterController{
                                LocalNodeID: 1,
                            },
                        }

                        bucketProxy, err := cloudBucketProxyFactory.CreateBucketProxy(1, "site1", "default")

                        Expect(bucketProxy).Should(BeNil())
                        Expect(err).Should(Equal(ENoLocalBucket))
                    })
                })

                Context("and the site has been added to the pool but the site does not contain the specified bucket", func() {
                    It("should return ENoLocalBucket", func() {
                        cloudBucketProxyFactory := &CloudBucketProxyFactory{
                            SitePool: &DummySitePool{
                                sites: map[string]Site{ 
                                    "site1": &DummySite{
                                        bucketList: NewBucketList(),
                                    },
                                },
                            },
                            ClusterController: &ClusterController{
                                LocalNodeID: 1,
                            },
                        }

                        bucketProxy, err := cloudBucketProxyFactory.CreateBucketProxy(1, "site1", "default")

                        Expect(bucketProxy).Should(BeNil())
                        Expect(err).Should(Equal(ENoLocalBucket))
                    })
                })

                Context("and the site has been added to the pool and it contains the specified bucket", func() {
                    It("should return a local bucket proxy for that bucket", func() {
                        bucket := &DummyBucket{
                            name: "default",
                        }

                        bucketList := NewBucketList()
                        bucketList.AddBucket(bucket)

                        cloudBucketProxyFactory := &CloudBucketProxyFactory{
                            SitePool: &DummySitePool{
                                sites: map[string]Site{ 
                                    "site1": &DummySite{
                                        bucketList: bucketList,
                                    },
                                },
                            },
                            ClusterController: &ClusterController{
                                LocalNodeID: 1,
                            },
                        }

                        bucketProxy, err := cloudBucketProxyFactory.CreateBucketProxy(1, "site1", "default")
                        _, ok := bucketProxy.(*LocalBucketProxy)

                        Expect(ok).Should(BeTrue())
                        Expect(bucketProxy).Should(Not(BeNil()))
                        Expect(err).Should(BeNil())
                    })
                })
            })
        })
    })

    Describe("LocalBucketProxy", func() {
        Describe("#Name", func() {
            Specify("Should return the result of Name of the Bucket it is a proxy for", func() {
                localBucketProxy := &LocalBucketProxy{
                    Bucket: &DummyBucket{
                        name: "default",
                    },
                }

                Expect(localBucketProxy.Name()).Should(Equal("default"))
            })
        })

        Describe("#MerkleTree", func() {
            Specify("Should return a LocalMerkleTreeProxy which proxy for the MerkleTree of the Bucket", func() {
                merkleTree, _ := NewMerkleTree(MerkleMinDepth)
                localBucketProxy := &LocalBucketProxy{
                    Bucket: &DummyBucket{
                        name: "default",
                        merkleTree: merkleTree,
                    },
                }

                merkleTreeProxy := localBucketProxy.MerkleTree()
                _, ok := merkleTreeProxy.(*LocalMerkleTreeProxy)

                Expect(ok).Should(BeTrue())
                Expect(merkleTreeProxy.(*LocalMerkleTreeProxy).MerkleTree()).Should(Equal(merkleTree))
            })
        })

        Describe("#GetSyncChildren", func() {
            Specify("Should return the result of GetSyncChildren of the Bucket it is a proxy for", func() {
               localBucketProxy := &LocalBucketProxy{
                    Bucket: &DummyBucket{
                        name: "default",
                    },
                }

                iter, err := localBucketProxy.GetSyncChildren(0)

                Expect(iter).Should(BeNil())
                Expect(err).Should(BeNil())
            })
        })

        Describe("#Close", func() {
            Specify("Should release the associated site in the site pool", func() {
                localBucketProxy := &LocalBucketProxy{
                    SitePool: &DummySitePool{
                    },
                    Bucket: &DummyBucket{
                        name: "default",
                    },
                    SiteID: "site1",
                }

                localBucketProxy.Close()

                Expect(localBucketProxy.SitePool.(*DummySitePool).released["site1"]).Should(Equal(1))
            })
        })
    })

    Describe("RemoteBucketProxy", func() {
    })
})