package transfer_test

import (
    "io"
    "net/http"
    "time"

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
})
