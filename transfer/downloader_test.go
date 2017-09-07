package transfer_test

import (
    "io"
    "time"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/raft"
    . "devicedb/transfer"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("Downloader", func() {
    Describe("#Download", func() {
        Context("When this node is already a holder for this partition", func() {
            var configController *ConfigController
            var downloader *Downloader

            BeforeEach(func() {
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: ClusterState{
                        ClusterSettings: ClusterSettings{
                            Partitions: 1024,
                            ReplicationFactor: 3,
                        },
                    },
                }
                clusterController.State.Initialize()
                clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 1 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
                clusterController.State.AssignPartitionReplica(0, 0, 1)
                configController = NewConfigController(nil, nil, clusterController)

                downloader = NewDownloader(configController, nil, nil, nil, nil)
            })

            It("Should return a closed done channel and make no download attempts", func() {
                done := downloader.Download(0)

                _, ok := <-done

                Expect(ok).Should(BeFalse())
                Expect(downloader.IsDownloading(0)).Should(BeFalse())
            })
        })

        Context("When there are no nodes which hold replicas for the specified partition", func() {
            var configController *ConfigController
            var downloader *Downloader

            BeforeEach(func() {
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: ClusterState{
                        ClusterSettings: ClusterSettings{
                            Partitions: 1024,
                            ReplicationFactor: 3,
                        },
                    },
                }
                clusterController.State.Initialize()
                clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 1 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
                configController = NewConfigController(nil, nil, clusterController)

                // initialize it with an empty mock transfer partner strategy so it will return 0
                // when ChooseTransferPartner() is called indicating there are no holders for this
                // partition
                downloader = NewDownloader(configController, nil, NewMockTransferPartnerStrategy(), nil, nil)
            })

            It("Should return a closed done channel and make no download attempts", func() {
                done := downloader.Download(0)

                _, ok := <-done

                Expect(ok).Should(BeFalse())
                Expect(downloader.IsDownloading(0)).Should(BeFalse())
            })
        })

        Context("When there is at least one node which holds a replica for the specified partition", func() {
            var configController *ConfigController

            BeforeEach(func() {
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: ClusterState{
                        ClusterSettings: ClusterSettings{
                            Partitions: 1024,
                            ReplicationFactor: 3,
                        },
                    },
                }
                clusterController.State.Initialize()
                clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 1 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
                clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 2 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
                clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 3 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
                // Set up cluster so nodes 2 and 3 hold replicas of partition 0 but node 1 doesn't hold any replicas
                clusterController.State.AssignPartitionReplica(0, 0, 2) // assign patition 0, replica 0 to node 2
                clusterController.State.AssignPartitionReplica(0, 1, 3) // assign patition 0, replica 1 to node 3
                configController = NewConfigController(nil, nil, clusterController)
            })

            Context("And there is no download currently in progress for this partition", func() {
                It("Should return a new done channel", func() {
                    // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                    downloader := NewDownloader(configController, NewMockTransferTransport(), NewMockTransferPartnerStrategy().AppendNextTransferPartner(2), nil, nil)
                    done := downloader.Download(0)

                    select {
                    case <-done:
                        Fail("Done channel should be open")
                    default:
                    }

                    downloader.CancelDownload(0)
                })

                It("Should choose a transfer partner from the list of nodes which hold a replica of this partition", func() {
                    chooseTransferPartnerCalled := make(chan int)
                    partnerStrategy := NewMockTransferPartnerStrategy()
                    partnerStrategy.AppendNextTransferPartner(2)
                    partnerStrategy.onChooseTransferPartner(func(partition uint64) {
                        defer GinkgoRecover()
                        Expect(partition).Should(Equal(uint64(0)))
                        chooseTransferPartnerCalled <- 1
                    })

                    // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                    downloader := NewDownloader(configController, NewMockTransferTransport(), partnerStrategy, nil, nil)
                    downloader.Download(0)

                    select {
                    case <-chooseTransferPartnerCalled:
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }

                    downloader.CancelDownload(0)
                })

                It("Should then initiate a download from its chosen transfer partner", func() {
                    getCalled := make(chan int)
                    partnerStrategy := NewMockTransferPartnerStrategy()
                    partnerStrategy.AppendNextTransferPartner(2)
                    transferTransport := NewMockTransferTransport()
                    transferTransport.onGet(func(node uint64, partition uint64) {
                        defer GinkgoRecover()
                        Expect(node).Should(Equal(uint64(2)))
                        Expect(partition).Should(Equal(uint64(0)))
                        getCalled <- 1
                    })

                    // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                    downloader := NewDownloader(configController, transferTransport, partnerStrategy, nil, nil)
                    downloader.Download(0)

                    select {
                    case <-getCalled:
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }

                    downloader.CancelDownload(0)
                })

                Context("And if there is a problem initiating a download from its chosen transfer partner", func() {
                    It("Should wait for some period of time before again choosing a transfer partner and attempting to restart the download", func() {
                        chooseTransferPartnerCalled := make(chan int)
                        getCalled := make(chan int)
                        partnerStrategy := NewMockTransferPartnerStrategy()
                        partnerStrategy.AppendNextTransferPartner(2)
                        partnerStrategy.AppendNextTransferPartner(2)
                        partnerStrategy.onChooseTransferPartner(func(partition uint64) {
                            defer GinkgoRecover()
                            Expect(partition).Should(Equal(uint64(0)))
                            chooseTransferPartnerCalled <- 1
                        })
                        // An empty mock transfer transport will always return an error when Get is called so no need to initialize with responses
                        transferTransport := NewMockTransferTransport()
                        transferTransport.onGet(func(node uint64, partition uint64) {
                            defer GinkgoRecover()
                            Expect(node).Should(Equal(uint64(2)))
                            Expect(partition).Should(Equal(uint64(0)))
                            getCalled <- 1
                        })

                        // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                        downloader := NewDownloader(configController, transferTransport, partnerStrategy, nil, nil)
                        downloader.Download(0)

                        select {
                        case <-chooseTransferPartnerCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        select {
                        case <-getCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        select {
                        case <-chooseTransferPartnerCalled:
                        // Wait period should be no more than 2 seconds
                        case <-time.After(time.Second * 2):
                            Fail("Test timed out")
                        }

                        select {
                        case <-getCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        downloader.CancelDownload(0)
                    })
                })

                Context("And if the download is started successfully", func() {
                    It("Should decode the read stream as an incoming transfer", func() {
                        createIncomingTransferCalled := make(chan int)
                        partnerStrategy := NewMockTransferPartnerStrategy()
                        partnerStrategy.AppendNextTransferPartner(2)
                        transferTransport := NewMockTransferTransport()
                        infiniteReader := NewInfiniteReader()
                        transferTransport.AppendNextGetResponse(infiniteReader, func() { }, nil)
                        incomingTransfer := NewMockPartitionTransfer()
                        incomingTransfer.AppendNextChunkResponse(PartitionChunk{ }, io.EOF)
                        transferFactory := NewMockPartitionTransferFactory()
                        transferFactory.AppendNextIncomingTransfer(incomingTransfer)
                        transferFactory.onCreateIncomingTransfer(func(reader io.Reader) {
                            defer GinkgoRecover()
                            Expect(reader.(*InfiniteReader)).Should(Equal(infiniteReader))
                            createIncomingTransferCalled <- 1
                        })

                        // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                        downloader := NewDownloader(configController, transferTransport, partnerStrategy, transferFactory, nil)
                        downloader.Download(0)

                        select {
                        case <-createIncomingTransferCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }                       

                        downloader.CancelDownload(0)
                    })

                    Specify("For each chunk sent, all entries in that chunk should be written to the partition", func() {
                        siteAMergeCalled := make(chan int)
                        siteBMergeCalled := make(chan int)

                        siteADefaultBucket := NewMockBucket("default")
                        siteADefaultBucket.AppendNextMergeResponse(nil)
                        siteADefaultBucket.AppendNextMergeResponse(nil)
                        siteADefaultBucket.onMerge(func(siblingSets map[string]*SiblingSet) {
                            defer GinkgoRecover()

                            Expect(len(siblingSets)).Should(Equal(1))
                            var key string

                            if siteADefaultBucket.mergeCalls == 1 {
                                key = "aa"
                            } else {
                                key = "bb"
                            }

                            _, ok := siblingSets[key]

                            Expect(ok).Should(BeTrue())

                            siteAMergeCalled <- 1
                        })
                        siteBDefaultBucket := NewMockBucket("default")
                        siteBDefaultBucket.AppendNextMergeResponse(nil)
                        siteBDefaultBucket.AppendNextMergeResponse(nil)
                        siteBDefaultBucket.onMerge(func(siblingSets map[string]*SiblingSet) {
                            defer GinkgoRecover()

                            Expect(len(siblingSets)).Should(Equal(1))
                            var key string

                            if siteBDefaultBucket.mergeCalls == 1 {
                                key = "aa"
                            } else {
                                key = "bb"
                            }

                            _, ok := siblingSets[key]

                            Expect(ok).Should(BeTrue())

                            siteBMergeCalled <- 1
                        })
                        siteABuckets := NewBucketList()
                        siteABuckets.AddBucket(siteADefaultBucket)
                        siteBBuckets := NewBucketList()
                        siteBBuckets.AddBucket(siteBDefaultBucket)
                        siteA := NewMockSite(siteABuckets)
                        siteB := NewMockSite(siteBBuckets)
                        sitePool := NewMockSitePool()
                        sitePool.AppendNextAcquireResponse(siteA)
                        sitePool.AppendNextAcquireResponse(siteA)
                        sitePool.AppendNextAcquireResponse(siteB)
                        sitePool.AppendNextAcquireResponse(siteB)
                        partition := NewMockPartition(0, 0)
                        partition.SetSites(sitePool)
                        partitionPool := NewMockPartitionPool()
                        partitionPool.Add(partition)
                        partnerStrategy := NewMockTransferPartnerStrategy()
                        partnerStrategy.AppendNextTransferPartner(2)
                        transferTransport := NewMockTransferTransport()
                        infiniteReader := NewInfiniteReader()
                        transferTransport.AppendNextGetResponse(infiniteReader, func() { }, nil)
                        incomingTransfer := NewMockPartitionTransfer()
                        incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 1, Checksum: Hash{ }, Entries: []Entry{ 
                            Entry{ 
                                Site: "siteA",
                                Bucket: "default",
                                Key: "aa",
                                Value: nil,
                            },
                            Entry{ 
                                Site: "siteA",
                                Bucket: "default",
                                Key: "bb",
                                Value: nil,
                            },
                        } }, nil)
                        incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 2, Checksum: Hash{ }, Entries: []Entry{ 
                            Entry{ 
                                Site: "siteB",
                                Bucket: "default",
                                Key: "aa",
                                Value: nil,
                            },
                            Entry{ 
                                Site: "siteB",
                                Bucket: "default",
                                Key: "bb",
                                Value: nil,
                            },
                        } }, nil)
                        incomingTransfer.AppendNextChunkResponse(PartitionChunk{ }, io.EOF)
                        transferFactory := NewMockPartitionTransferFactory()
                        transferFactory.AppendNextIncomingTransfer(incomingTransfer)

                        // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                        downloader := NewDownloader(configController, transferTransport, partnerStrategy, transferFactory, partitionPool)
                        downloader.Download(0)

                        select {
                        case <-siteAMergeCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        select {
                        case <-siteAMergeCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        select {
                        case <-siteBMergeCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        select {
                        case <-siteBMergeCalled:
                        case <-time.After(time.Second):
                            Fail("Test timed out")
                        }

                        downloader.CancelDownload(0)
                    })

                    Context("And if that partition is not in the partition pool", func() {
                        It("Should cause a panic", func() {
                            partitionPool := NewMockPartitionPool()
                            partnerStrategy := NewMockTransferPartnerStrategy()
                            partnerStrategy.AppendNextTransferPartner(2)
                            transferTransport := NewMockTransferTransport()
                            infiniteReader := NewInfiniteReader()
                            transferTransport.AppendNextGetResponse(infiniteReader, func() { }, nil)
                            incomingTransfer := NewMockPartitionTransfer()
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 1, Checksum: Hash{ }, Entries: []Entry{ 
                                Entry{ 
                                    Site: "siteA",
                                    Bucket: "default",
                                    Key: "aa",
                                    Value: nil,
                                },
                                Entry{ 
                                    Site: "siteA",
                                    Bucket: "default",
                                    Key: "bb",
                                    Value: nil,
                                },
                            } }, nil)
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 2, Checksum: Hash{ }, Entries: []Entry{ 
                                Entry{ 
                                    Site: "siteB",
                                    Bucket: "default",
                                    Key: "aa",
                                    Value: nil,
                                },
                                Entry{ 
                                    Site: "siteB",
                                    Bucket: "default",
                                    Key: "bb",
                                    Value: nil,
                                },
                            } }, nil)
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ }, io.EOF)
                            transferFactory := NewMockPartitionTransferFactory()
                            transferFactory.AppendNextIncomingTransfer(incomingTransfer)
    
                            // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                            panicHappened := make(chan int)
                            downloader := NewDownloader(configController, transferTransport, partnerStrategy, transferFactory, partitionPool)
                            downloader.OnPanic(func(p interface{}) {
                                panicHappened <- 1
                            })
                            downloader.Download(0)

                            select {
                            case <-panicHappened:
                            case <-time.After(time.Second):
                                Fail("Test timed out")
                            }
                            
                            downloader.CancelDownload(0)                           
                        })
                    })

                    Context("And if that bucket is not in the partition", func() {
                        It("Should cause a panic", func() {
                            siteABuckets := NewBucketList()
                            siteBBuckets := NewBucketList()
                            siteA := NewMockSite(siteABuckets)
                            siteB := NewMockSite(siteBBuckets)
                            sitePool := NewMockSitePool()
                            sitePool.AppendNextAcquireResponse(siteA)
                            sitePool.AppendNextAcquireResponse(siteA)
                            sitePool.AppendNextAcquireResponse(siteB)
                            sitePool.AppendNextAcquireResponse(siteB)
                            partition := NewMockPartition(0, 0)
                            partition.SetSites(sitePool)
                            partitionPool := NewMockPartitionPool()
                            partitionPool.Add(partition)
                            partnerStrategy := NewMockTransferPartnerStrategy()
                            partnerStrategy.AppendNextTransferPartner(2)
                            transferTransport := NewMockTransferTransport()
                            infiniteReader := NewInfiniteReader()
                            transferTransport.AppendNextGetResponse(infiniteReader, func() { }, nil)
                            incomingTransfer := NewMockPartitionTransfer()
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 1, Checksum: Hash{ }, Entries: []Entry{ 
                                Entry{ 
                                    Site: "siteA",
                                    Bucket: "default",
                                    Key: "aa",
                                    Value: nil,
                                },
                                Entry{ 
                                    Site: "siteA",
                                    Bucket: "default",
                                    Key: "bb",
                                    Value: nil,
                                },
                            } }, nil)
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ Index: 2, Checksum: Hash{ }, Entries: []Entry{ 
                                Entry{ 
                                    Site: "siteB",
                                    Bucket: "default",
                                    Key: "aa",
                                    Value: nil,
                                },
                                Entry{ 
                                    Site: "siteB",
                                    Bucket: "default",
                                    Key: "bb",
                                    Value: nil,
                                },
                            } }, nil)
                            incomingTransfer.AppendNextChunkResponse(PartitionChunk{ }, io.EOF)
                            transferFactory := NewMockPartitionTransferFactory()
                            transferFactory.AppendNextIncomingTransfer(incomingTransfer)

                            // download won't be able to make progress since the transfer strategy will return an error. That way the done channel won't close
                            panicHappened := make(chan int)
                            downloader := NewDownloader(configController, transferTransport, partnerStrategy, transferFactory, partitionPool)
                            downloader.OnPanic(func(p interface{}) {
                                panicHappened <- 1
                            })
                            downloader.Download(0)

                            select {
                            case <-panicHappened:
                            case <-time.After(time.Second):
                                Fail("Test timed out")
                            }

                            downloader.CancelDownload(0)
                        })
                    })

                    Context("And if that site is not in the partition at the local node", func() {
                        It("Should ensure the transport cleans up by calling its closer function", func() {
                            Fail("Not implemented")
                        })

                        It("Should wait for some period of time before again choosing a transfer partner and attempting to restart the download", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if an error occurs while writing to the partition", func() {
                        It("Should ensure the transport cleans up by calling its closer function", func() {
                            Fail("Not implemented")
                        })

                        It("Should wait for some period of time before again choosing a transfer partner and attempting to restart the download", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if there is a problem encountered while decoding the incoming transfer", func() {
                        It("Should ensure the transport cleans up by calling its closer function", func() {
                            Fail("Not implemented")
                        })

                        It("Should wait for some period of time before again choosing a transfer partner and attempting to restart the download", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And when the download is finished successfully", func() {
                        It("Should ensure the transport cleans up by calling its closer function", func() {
                            Fail("Not implemented")
                        })

                        It("Should close the done channel for this partition download", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("And if Cancel is called for that partition while the download is in progress", func() {
                        It("Should call Cancel on the incoming transfer", func() {
                            Fail("Not implemented")
                        })

                        It("Should ensure the transport cleans up by calling its closer function", func() {
                            Fail("Not implemented")
                        })

                        It("Should stop all attempts to download the partition", func() {
                            Fail("Not implemented")
                        })

                        It("Should leave the done channel open", func() {
                            Fail("Not implemented")
                        })
                    })
                })
            })

            Context("And there is already a download in progress for this partition", func() {
                It("Should return the done channel for that partition", func() {
                    Fail("Not implemented")
                })
            })
            
            Context("And a download already successfully completed for this partition", func() {
                It("Should return the done channel for that partition which should be closed", func() {
                    Fail("Not implemented")
                })
            })
        })
    })

    Describe("#Cancel", func() {
        Context("When there is no download in progress for the specified partition", func() {
            It("Should do nothing", func() {
                Fail("Not implemented")
            })
        })

        Context("When there is a download in progress for the specified partition", func() {
            It("Should call the Cancel function on the canceller for that download", func() {
                Fail("Not implemented")
            })

            It("Should remove that download", func() {
                Fail("Not implemented")
            })
        })
    })
})
