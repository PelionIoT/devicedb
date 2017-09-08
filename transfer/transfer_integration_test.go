package transfer_test

import (
    "io"
    "net/http"
    "time"

    . "devicedb/bucket"
    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/raft"
    . "devicedb/transfer"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("TransferIntegrationTest", func() {
    // Test the full pipeline starting out with a partition iterator, transforming it into a read stream and finally
    // re-encoding it into a series of partition chunks
    Describe("Partition Chunks -> Outgoing transfer -> Transfer Encoder -> Incoming Transfer -> Partition Chunks", func() {
        Context("Without a network layer in between", func() {
            Specify("Should encode entries from the partition into a byte stream and decode it on the other end into the same series of entries", func () {
                partition := NewMockPartition(0, 0)
                iterator := partition.MockIterator()
                iterator.AppendNextState(true, "site1", "default", "a", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "b", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "c", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "d", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "e", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "f", nil, Hash{}, nil)
                iterator.AppendNextState(false, "", "", "", nil, Hash{}, nil)

                outgoingTransfer := NewOutgoingTransfer(partition, 2)
                transferEncoder := NewTransferEncoder(outgoingTransfer)
                r, _ := transferEncoder.Encode()
                transferDecoder := NewTransferDecoder(r)
                incomingTransfer, _ := transferDecoder.Decode()

                nextChunk, err := incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 1,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "a",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "b",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 2,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "c",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "d",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 3,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "e",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "f",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk.IsEmpty()).Should(BeTrue())
                Expect(err).Should(Equal(io.EOF))
            })
        })

        Context("With a network layer in between", func() {
            Specify("Should encode entries from the partition into a byte stream and decode it on the other end into the same series of entries", func () {
                partition := NewMockPartition(0, 0)
                iterator := partition.MockIterator()
                iterator.AppendNextState(true, "site1", "default", "a", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "b", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "c", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "d", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "e", nil, Hash{}, nil)
                iterator.AppendNextState(true, "site1", "default", "f", nil, Hash{}, nil)
                iterator.AppendNextState(false, "", "", "", nil, Hash{}, nil)

                outgoingTransfer := NewOutgoingTransfer(partition, 2)
                transferEncoder := NewTransferEncoder(outgoingTransfer)
                encodedStream, _ := transferEncoder.Encode()

                testServer := NewHTTPTestServer(9090, &StringResponseHandler{ str: encodedStream })
                clusterController := &ClusterController{
                    LocalNodeID: 1,
                    State: ClusterState{
                        Nodes: map[uint64]*NodeConfig{
                            1: &NodeConfig{
                                Address: PeerAddress{
                                    NodeID: 1,
                                    Host: "localhost",
                                    Port: 9090,
                                },
                            },
                        },
                    },
                }
                configController := NewConfigController(nil, nil, clusterController)
                httpClient := &http.Client{}
                transferTransport := NewHTTPTransferTransport(configController, httpClient)
                testServer.Start()
                // give it enough time to fully start
                <-time.After(time.Second)
                r, cancel, err := transferTransport.Get(1, 0)

                transferDecoder := NewTransferDecoder(r)
                incomingTransfer, _ := transferDecoder.Decode()

                nextChunk, err := incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 1,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "a",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "b",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 2,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "c",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "d",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk).Should(Equal(PartitionChunk{
                    Index: 3,
                    Checksum: Hash{ },
                    Entries: []Entry{
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "e",
                            Value: nil,
                        },
                        Entry{
                            Site: "site1",
                            Bucket: "default",
                            Key: "f",
                            Value: nil,
                        },
                    },
                }))
                Expect(err).Should(BeNil())
                nextChunk, err = incomingTransfer.NextChunk()
                Expect(nextChunk.IsEmpty()).Should(BeTrue())
                Expect(err).Should(Equal(io.EOF))

                cancel()
                testServer.Stop()
            })
        })
    })

    // Test a partition transfer from download to proposal
    // Ensuring correct ordering and interaction between downloader and
    // Transfer proposer 
    Describe("Downloader Process", func() {
        Specify("The downloader should request a partition transfer from the current holder of the partition replica and write it do a partition", func() {
            partition := NewMockPartition(0, 0)
            iterator := partition.MockIterator()
            iterator.AppendNextState(true, "site1", "default", "a", nil, Hash{}, nil)
            iterator.AppendNextState(true, "site1", "default", "b", nil, Hash{}, nil)
            iterator.AppendNextState(true, "site1", "default", "c", nil, Hash{}, nil)
            iterator.AppendNextState(true, "site1", "default", "d", nil, Hash{}, nil)
            iterator.AppendNextState(true, "site1", "default", "e", nil, Hash{}, nil)
            iterator.AppendNextState(true, "site1", "default", "f", nil, Hash{}, nil)
            iterator.AppendNextState(false, "", "", "", nil, Hash{}, nil)

            outgoingTransfer := NewOutgoingTransfer(partition, 2)
            transferEncoder := NewTransferEncoder(outgoingTransfer)
            encodedStream, _ := transferEncoder.Encode()

            testServer := NewHTTPTestServer(7070, &StringResponseHandler{ str: encodedStream })
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
            clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 1, Host: "localhost", Port: 6060 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
            clusterController.State.AddNode(NodeConfig{ Address: PeerAddress{ NodeID: 2, Host: "localhost", Port: 7070 }, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{ } })
            clusterController.State.AssignPartitionReplica(0, 0, 2)
            configController := NewConfigController(nil, nil, clusterController)
            httpClient := &http.Client{}
            transferTransport := NewHTTPTransferTransport(configController, httpClient)
            partnerStrategy := NewRandomTransferPartnerStrategy(configController)
            transferFactory := &TransferFactory{ }
            localDefaultBucket := NewMockBucket("default")
            localDefaultBucket.SetDefaultMergeResponse(nil)
            bucketList := NewBucketList()
            bucketList.AddBucket(localDefaultBucket)
            localSite1 := NewMockSite(bucketList)
            localSitePool := NewMockSitePool()
            localSitePool.AppendNextAcquireResponse(localSite1)
            localSitePool.AppendNextAcquireResponse(localSite1)
            localSitePool.AppendNextAcquireResponse(localSite1)
            localSitePool.AppendNextAcquireResponse(localSite1)
            localSitePool.AppendNextAcquireResponse(localSite1)
            localSitePool.AppendNextAcquireResponse(localSite1)
            localPartition := NewMockPartition(0, 0)
            localPartition.SetSites(localSitePool)
            partitionPool := NewMockPartitionPool()
            partitionPool.Add(localPartition)
            nextMerge := make(chan map[string]*SiblingSet)
            downloader := NewDownloader(configController, transferTransport, partnerStrategy, transferFactory, partitionPool)
            localDefaultBucket.onMerge(func(siblingSets map[string]*SiblingSet) {
                nextMerge <- siblingSets
            })
            testServer.Start()
            defer testServer.Stop()

            // give it enough time to fully start
            <-time.After(time.Second)

            keys := []string{ "a", "b", "c", "d", "e", "f" }
            done := downloader.Download(0)
           
            for _, key := range keys {
                select {
                case n := <-nextMerge:
                    Expect(len(n)).Should(Equal(1))
                    _, ok := n[key]
                    Expect(ok).Should(BeTrue())
                case <-time.After(time.Second):
                    Fail("Test timed out")
                }
            }

            select {
            case <-done:
            case <-time.After(time.Second):
                Fail("Test timed out")
            }
        })
    })
})
