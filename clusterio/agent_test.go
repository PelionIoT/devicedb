package clusterio_test

import (
    "context"
    "errors"
    "sync"
    "time"

    . "devicedb/bucket"
    . "devicedb/clusterio"
    . "devicedb/data"
    . "devicedb/error"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("Agent", func() {
    Describe("#NQuorum", func() {
        It("Should return the number of replicas necessary to achieve a majority", func() {
            agent := NewAgent()

            Expect(agent.NQuorum(1)).Should(Equal(1))
            Expect(agent.NQuorum(2)).Should(Equal(2))
            Expect(agent.NQuorum(3)).Should(Equal(2))
            Expect(agent.NQuorum(4)).Should(Equal(3))
            Expect(agent.NQuorum(5)).Should(Equal(3))
            Expect(agent.NQuorum(6)).Should(Equal(4))
            Expect(agent.NQuorum(7)).Should(Equal(4))
        })
    })

    Describe("#Batch", func() {
        It("Should call Partition() on the siteID passed to it to obtain the partition number for this site", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionCalled := make(chan int, 1)
            partitionResolver.partitionCB = func(siteID string) {
                Expect(siteID).Should(Equal("site1"))
                partitionCalled <- 1
            }
            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient

            agent.Batch(context.TODO(), "site1", "default", nil)

            select {
            case <-partitionCalled:
            default:
                Fail("Should have invoked Partition()")
            }
        })

        It("Should use the result of its call to Partition() as the parameter of its call to ReplicaNodes", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionResolver.defaultPartitionResponse = 500
            replicaNodesCalled := make(chan int, 1)
            partitionResolver.replicaNodesCB = func(partition uint64) {
                Expect(partition).Should(Equal(uint64(500)))
                replicaNodesCalled <- 1
            }
            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient

            agent.Batch(context.TODO(), "site1", "default", nil)

            select {
            case <-replicaNodesCalled:
            default:
                Fail("Should have invoked ReplicaNodes()")
            }
        })

        It("Should call NodeClient.Batch() once for each node returned by its call to ReplicaNodes", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionResolver.defaultPartitionResponse = 500
            partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
            nodeClientBatchCalled := make(chan int, 3)
            var mapMutex sync.Mutex
            remainingNodes := map[uint64]bool{ 2: true, 4: true, 6: true }
            nodeClient.batchCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
                defer GinkgoRecover()

                mapMutex.Lock()
                defer mapMutex.Unlock()
                _, ok := remainingNodes[nodeID]
                Expect(ok).Should(BeTrue())
                delete(remainingNodes, nodeID)
                Expect(partition).Should(Equal(uint64(500)))
                Expect(siteID).Should(Equal("site1"))
                Expect(bucket).Should(Equal("default"))

                nodeClientBatchCalled <- 1

                return nil
            }

            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient

            agent.Batch(context.TODO(), "site1", "default", nil)

            for i := 0; i < 3; i += 1 {
                select {
                case <-nodeClientBatchCalled:
                case <-time.After(time.Second):
                    Fail("Should have invoked NodeClient.Batch()")
                }
            }
        })

        Context("When the deadline specified by Timeout is reached before all calls to NodeClient.Batch() have returned", func() {
            Context("And a write quorum has not yet been established", func() {
                // Before the deadline quorum has not been reached and there are nodes that have not yet responded
                // After the deadline all outstanding calls to NodeClient.Batch() should be cancelled causing Batch()
                // to return
                It("Should not return until after the deadline is reached", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClientBatchCalled := make(chan int, 3)
                    nodeClient.defaultBatchResponse = nil
                    nodeClient.batchCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
                        if nodeID == 2 {
                            nodeClientBatchCalled <- 1
                            return nil
                        }

                        // all calls to batch except the one for node 2 should wait until past the deadline to return
                        <-ctx.Done()
                        nodeClientBatchCalled <- 1

                        return errors.New("Some error")
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.Timeout = time.Second // deadline is one second

                    batchReturned := make(chan int)
                    var batchCallTime time.Time

                    go func() {
                        defer GinkgoRecover()

                        batchCallTime = time.Now()
                        nReplicas, nApplied, err := agent.Batch(context.TODO(), "site1", "default", nil)

                        Expect(nReplicas).Should(Equal(3))
                        Expect(nApplied).Should(Equal(1))
                        Expect(err).Should(Equal(ENoQuorum))

                        batchReturned <- 1
                    }()

                    select {
                    case <-nodeClientBatchCalled:
                    case <-time.After(time.Millisecond * 100):
                        Fail("Should have finished calling batch for node 2")
                    }

                    select {
                    case <-batchReturned:
                        // ensure that the time since calling batch has been at least one second (the deadline)
                        // with an upper limit to the variance
                        Expect(time.Since(batchCallTime) > time.Second).Should(BeTrue())
                        Expect(time.Since(batchCallTime) < time.Second + time.Millisecond * 100).Should(BeTrue())
                    case <-time.After(agent.Timeout * 2):
                        Fail("Batch didn't return in time")
                    }

                    for i := 0; i < 2; i += 1 {
                        select {
                        case <-nodeClientBatchCalled:
                        case <-time.After(time.Millisecond * 100):
                            Fail("Batch did not return in time")
                        }
                    }
                })
            })

            Context("And a write quorum has already been established", func() {
                It("Should return before the deadline as soon as quorum has been established", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClientBatchCalled := make(chan int, 3)
                    nodeClient.defaultBatchResponse = nil
                    nodeClient.batchCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
                        if nodeID == 2 || nodeID == 4 {
                            nodeClientBatchCalled <- 1
                            return nil
                        }

                        // all calls to batch except the one for node 2 should wait until past the deadline to return
                        <-ctx.Done()
                        nodeClientBatchCalled <- 1
                        return errors.New("Some error")
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.Timeout = time.Second // deadline is one second

                    batchReturned := make(chan int)
                    var batchCallTime time.Time

                    go func() {
                        defer GinkgoRecover()

                        batchCallTime = time.Now()
                        nReplicas, nApplied, err := agent.Batch(context.TODO(), "site1", "default", nil)

                        Expect(nReplicas).Should(Equal(3))
                        Expect(nApplied).Should(Equal(2))
                        Expect(err).Should(BeNil())

                        batchReturned <- 1
                    }()

                    for i := 0; i < 2; i += 1 {
                        select {
                        case <-nodeClientBatchCalled:
                        case <-time.After(time.Millisecond * 100):
                            Fail("Batch did not return in time")
                        }
                    }

                    select {
                    case <-batchReturned:
                        // Batch should basically return right away since there are no timeouts in the critical path
                        Expect(time.Since(batchCallTime) < time.Millisecond * 100).Should(BeTrue())
                    case <-time.After(agent.Timeout * 2):
                        Fail("Batch didn't return in time")
                    }
                })
            })
        })

        Context("When all calls to NodeClient.Batch() return before the deadline", func() {
            Context("And a write quorum was established", func() {
                It("Should return as soon as quorum has been established", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClientBatchCalled := make(chan int, 3)
                    nodeClient.defaultBatchResponse = nil
                    nodeClient.batchCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
                        if nodeID == 2 || nodeID == 4 {
                            nodeClientBatchCalled <- 1
                            return nil
                        }

                        // all calls to batch except the one for node 2 should wait until past the deadline to return
                        nodeClientBatchCalled <- 1
                        return errors.New("Some error")
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.Timeout = time.Second // deadline is one second

                    batchReturned := make(chan int)
                    var batchCallTime time.Time

                    go func() {
                        defer GinkgoRecover()

                        batchCallTime = time.Now()
                        nReplicas, nApplied, err := agent.Batch(context.TODO(), "site1", "default", nil)

                        Expect(err).Should(BeNil())
                        Expect(nReplicas).Should(Equal(3))
                        Expect(nApplied).Should(Equal(2))

                        batchReturned <- 1
                    }()

                    for i := 0; i < 2; i += 1 {
                        select {
                        case <-nodeClientBatchCalled:
                        case <-time.After(time.Millisecond * 100):
                            Fail("Batch did not return in time")
                        }
                    }

                    select {
                    case <-batchReturned:
                        // Batch should basically return right away since there are no timeouts in the critical path
                        Expect(time.Since(batchCallTime) < time.Millisecond * 100).Should(BeTrue())
                    case <-time.After(agent.Timeout * 2):
                        Fail("Batch didn't return in time")
                    }
                })
            })

            Context("And a write quorum was not established", func() {
                It("Should return as soon as all calls to NodeClient.Batch() have returned", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClientBatchCalled := make(chan int, 3)
                    nodeClient.defaultBatchResponse = nil
                    nodeClient.batchCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, updateBatch *UpdateBatch) error {
                        if nodeID == 2 {
                            nodeClientBatchCalled <- 1
                            return nil
                        }

                        // all calls to batch except the one for node 2 should wait until past the deadline to return
                        nodeClientBatchCalled <- 1
                        return errors.New("Some error")
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.Timeout = time.Second // deadline is one second

                    batchReturned := make(chan int)
                    var batchCallTime time.Time

                    go func() {
                        defer GinkgoRecover()

                        batchCallTime = time.Now()
                        nReplicas, nApplied, err := agent.Batch(context.TODO(), "site1", "default", nil)

                        Expect(nReplicas).Should(Equal(3))
                        Expect(nApplied).Should(Equal(1))
                        Expect(err).Should(Equal(ENoQuorum))

                        batchReturned <- 1
                    }()

                    for i := 0; i < 2; i += 1 {
                        select {
                        case <-nodeClientBatchCalled:
                        case <-time.After(time.Millisecond * 100):
                            Fail("Batch did not return in time")
                        }
                    }

                    select {
                    case <-batchReturned:
                        // Batch should basically return right away since there are no timeouts in the critical path
                        Expect(time.Since(batchCallTime) < time.Millisecond * 100).Should(BeTrue())
                    case <-time.After(agent.Timeout * 2):
                        Fail("Batch didn't return in time")
                    }
                })
            })
        })
    })

    Describe("#Get", func() {
        sibling1 := NewSibling(NewDVV(NewDot("r1", 1), map[string]uint64{ "r2": 5, "r3": 2 }), []byte("v1"), 0)
        sibling2 := NewSibling(NewDVV(NewDot("r1", 2), map[string]uint64{ "r2": 4, "r3": 3 }), []byte("v2"), 0)
        sibling3 := NewSibling(NewDVV(NewDot("r2", 6), map[string]uint64{ }), []byte("v3"), 0)
        
        siblingSet1 := NewSiblingSet(map[*Sibling]bool{
            sibling1: true,
            sibling2: true, // makes v5 obsolete
            sibling3: true,
        })
        
        sibling4 := NewSibling(NewDVV(NewDot("r2", 7), map[string]uint64{ "r2": 6 }), []byte("v4"), 0)
        sibling5 := NewSibling(NewDVV(NewDot("r3", 1), map[string]uint64{ }), []byte("v5"), 0)
        
        siblingSet2 := NewSiblingSet(map[*Sibling]bool{
            sibling1: true,
            sibling4: true, // makes v3 obsolete
            sibling5: true,
        })

        It("Should call Partition() on the siteID passed to it to obtain the partition number for this site", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionCalled := make(chan int, 1)
            partitionResolver.partitionCB = func(siteID string) {
                Expect(siteID).Should(Equal("site1"))
                partitionCalled <- 1
            }
            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient
            agent.NodeReadRepairer = NewMockNodeReadRepairer()

            agent.Get(context.TODO(), "site1", "default", [][]byte{ })

            select {
            case <-partitionCalled:
            default:
                Fail("Should have invoked Partition()")
            }
        })

        It("Should use the result of its call to Partition() as the parameter of its call to ReplicaNodes", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionResolver.defaultPartitionResponse = 500
            replicaNodesCalled := make(chan int, 1)
            partitionResolver.replicaNodesCB = func(partition uint64) {
                Expect(partition).Should(Equal(uint64(500)))
                replicaNodesCalled <- 1
            }
            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient
            agent.NodeReadRepairer = NewMockNodeReadRepairer()

            agent.Get(context.TODO(), "site1", "default", [][]byte{ })

            select {
            case <-replicaNodesCalled:
            default:
                Fail("Should have invoked ReplicaNodes()")
            }
        })

        It("Should call NodeClient.Get() once for each node returned by its call to ReplicaNodes", func() {
            partitionResolver := NewMockPartitionResolver()
            nodeClient := NewMockNodeClient()
            partitionResolver.defaultPartitionResponse = 500
            partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
            nodeClientGetCalled := make(chan int, 3)
            var mapMutex sync.Mutex
            remainingNodes := map[uint64]bool{ 2: true, 4: true, 6: true }
            nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                defer GinkgoRecover()

                mapMutex.Lock()
                defer mapMutex.Unlock()
                _, ok := remainingNodes[nodeID]
                Expect(ok).Should(BeTrue())
                delete(remainingNodes, nodeID)
                Expect(partition).Should(Equal(uint64(500)))
                Expect(siteID).Should(Equal("site1"))
                Expect(bucket).Should(Equal("default"))
                Expect(keys).Should(Equal([][]byte{ []byte("a"), []byte("b"), []byte("c") }))

                nodeClientGetCalled <- 1

                return []*SiblingSet{ nil, nil, nil }, nil
            }

            agent := NewAgent()
            agent.PartitionResolver = partitionResolver
            agent.NodeClient = nodeClient
            agent.NodeReadRepairer = NewMockNodeReadRepairer()

            agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

            for i := 0; i < 3; i += 1 {
                select {
                case <-nodeClientGetCalled:
                case <-time.After(time.Second):
                    Fail("Should have invoked NodeClient.Get()")
                }
            }
        })
        
        Context("When the deadline specified by Timeout is reached before all calls to NodeClient.Batch() have returned", func() {
            It("Should call NodeReadRepairer.BeginRepair() as soon as the deadline is reached", func() {
                partitionResolver := NewMockPartitionResolver()
                nodeClient := NewMockNodeClient()
                nodeReadRepairer := NewMockNodeReadRepairer()
                partitionResolver.defaultPartitionResponse = 500
                partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                    switch nodeID {
                    case 2:
                        return []*SiblingSet{ siblingSet1, siblingSet2, nil }, nil
                    case 4:
                        return nil, errors.New("Some error")
                    case 6:
                        <-ctx.Done()
                        return nil, errors.New("Cancelled")
                    }

                    return nil, nil
                }

                var callStartTime time.Time

                beginRepairCalled := make(chan int)
                nodeReadRepairer.beginRepairCB = func(readMerger NodeReadMerger) {
                    defer GinkgoRecover()

                    Expect(time.Since(callStartTime) > time.Second).Should(BeTrue())
                    Expect(time.Since(callStartTime) < time.Second + time.Millisecond * 100).Should(BeTrue())
                    Expect(readMerger.Get("a")).Should(Equal(siblingSet1))
                    Expect(readMerger.Get("b")).Should(Equal(siblingSet2))
                    Expect(readMerger.Get("c")).Should(BeNil())

                    beginRepairCalled <- 1
                }

                agent := NewAgent()
                agent.PartitionResolver = partitionResolver
                agent.NodeClient = nodeClient
                agent.NodeReadRepairer = nodeReadRepairer
                agent.Timeout = time.Second // deadline is one second

                getReturned := make(chan int)

                go func() {
                    defer GinkgoRecover()

                    callStartTime = time.Now()
                    siblingSets, err := agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

                    Expect(siblingSets).Should(BeNil())
                    Expect(err).Should(Equal(ENoQuorum))

                    getReturned <- 1
                }()

                select {
                case <-beginRepairCalled:
                case <-time.After(time.Second * 2):
                    Fail("BeginRepair wasn't called")
                }

                select {
                case <-getReturned:
                case <-time.After(time.Second):
                    Fail("Get didn't return in time")
                }
            })

            Context("And a read quorum has not yet been established", func() {
                It("Should not return until the deadline is reached", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    nodeReadRepairer := NewMockNodeReadRepairer()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                        switch nodeID {
                        case 2:
                            return []*SiblingSet{ siblingSet1, siblingSet2, nil }, nil
                        case 4:
                            return nil, errors.New("Some error")
                        case 6:
                            <-ctx.Done()
                            return nil, errors.New("Cancelled")
                        }

                        return nil, nil
                    }

                    var callStartTime time.Time

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.NodeReadRepairer = nodeReadRepairer
                    agent.Timeout = time.Second // deadline is one second

                    getReturned := make(chan int)

                    go func() {
                        defer GinkgoRecover()

                        callStartTime = time.Now()
                        siblingSets, err := agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

                        Expect(siblingSets).Should(BeNil())
                        Expect(err).Should(Equal(ENoQuorum))

                        Expect(time.Since(callStartTime) > time.Second).Should(BeTrue())
                        Expect(time.Since(callStartTime) < time.Second + time.Millisecond * 100).Should(BeTrue())

                        getReturned <- 1
                    }()

                    select {
                    case <-getReturned:
                    case <-time.After(time.Second * 2):
                        Fail("Get didn't return in time")
                    }
                })
            })

            Context("And a read quorum has already been established", func() {
                It("Should return before the deadline as soon as quorum has been established", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    nodeReadRepairer := NewMockNodeReadRepairer()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                        switch nodeID {
                        case 2:
                            return []*SiblingSet{ siblingSet1, siblingSet2, nil }, nil
                        case 4:
                            return []*SiblingSet{ siblingSet2, siblingSet1, siblingSet1 }, nil
                        case 6:
                            <-ctx.Done()
                            return nil, errors.New("Cancelled")
                        }

                        return nil, nil
                    }

                    var callStartTime time.Time

                    beginRepairCalled := make(chan int)
                    nodeReadRepairer.beginRepairCB = func(readMerger NodeReadMerger) {
                        defer GinkgoRecover()

                        Expect(time.Since(callStartTime) > time.Second).Should(BeTrue())
                        Expect(time.Since(callStartTime) < time.Second + time.Millisecond * 100).Should(BeTrue())
                        Expect(readMerger.Get("a")).Should(Equal(siblingSet1.Sync(siblingSet2)))
                        Expect(readMerger.Get("b")).Should(Equal(siblingSet1.Sync(siblingSet2)))
                        Expect(readMerger.Get("c")).Should(Equal(siblingSet1))

                        beginRepairCalled <- 1
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.NodeReadRepairer = nodeReadRepairer
                    agent.Timeout = time.Second // deadline is one second

                    getReturned := make(chan int)

                    go func() {
                        defer GinkgoRecover()

                        callStartTime = time.Now()
                        siblingSets, err := agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

                        Expect(siblingSets).Should(Equal([]*SiblingSet{ siblingSet1.Sync(siblingSet2), siblingSet1.Sync(siblingSet2), siblingSet1 }))
                        Expect(err).Should(BeNil())
                        Expect(time.Since(callStartTime) < time.Millisecond * 100).Should(BeTrue())

                        getReturned <- 1
                    }()

                    select {
                    case <-getReturned:
                    case <-time.After(time.Second):
                        Fail("Get didn't return in time")
                    }

                    select {
                    case <-beginRepairCalled:
                    case <-time.After(time.Second * 2):
                        Fail("BeginRepair wasn't called")
                    }
                })
            })
        })

        Context("When all calls to NodeClient.Get() return before the deadline", func() {
            It("Should call NodeReadRepairer.BeginRepair() as soon as all calls to Get complete", func() {
                partitionResolver := NewMockPartitionResolver()
                nodeClient := NewMockNodeClient()
                nodeReadRepairer := NewMockNodeReadRepairer()
                partitionResolver.defaultPartitionResponse = 500
                partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                    switch nodeID {
                    case 2:
                        return []*SiblingSet{ siblingSet1, siblingSet2, nil }, nil
                    case 4:
                        return []*SiblingSet{ siblingSet2, siblingSet1, siblingSet1 }, nil
                    case 6:
                        return nil, errors.New("Cancelled")
                    }

                    return nil, nil
                }

                var callStartTime time.Time

                beginRepairCalled := make(chan int)
                nodeReadRepairer.beginRepairCB = func(readMerger NodeReadMerger) {
                    defer GinkgoRecover()

                    Expect(time.Since(callStartTime) < time.Millisecond * 100).Should(BeTrue())
                    Expect(readMerger.Get("a")).Should(Equal(siblingSet1.Sync(siblingSet2)))
                    Expect(readMerger.Get("b")).Should(Equal(siblingSet1.Sync(siblingSet2)))
                    Expect(readMerger.Get("c")).Should(Equal(siblingSet1))

                    beginRepairCalled <- 1
                }

                agent := NewAgent()
                agent.PartitionResolver = partitionResolver
                agent.NodeClient = nodeClient
                agent.NodeReadRepairer = nodeReadRepairer
                agent.Timeout = time.Second // deadline is one second

                getReturned := make(chan int)

                go func() {
                    defer GinkgoRecover()

                    callStartTime = time.Now()
                    siblingSets, err := agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

                    Expect(siblingSets).Should(Equal([]*SiblingSet{ siblingSet1.Sync(siblingSet2), siblingSet1.Sync(siblingSet2), siblingSet1 }))
                    Expect(err).Should(BeNil())
                    Expect(time.Since(callStartTime) < time.Millisecond * 100).Should(BeTrue())

                    getReturned <- 1
                }()

                select {
                case <-getReturned:
                case <-time.After(time.Second):
                    Fail("Get didn't return in time")
                }

                select {
                case <-beginRepairCalled:
                case <-time.After(time.Second * 2):
                    Fail("BeginRepair wasn't called")
                }
            })

            Context("And a read quorum was not established", func() {
                It("Should return as soon as all calls to NodeClient.Get() have returned", func() {
                    partitionResolver := NewMockPartitionResolver()
                    nodeClient := NewMockNodeClient()
                    nodeReadRepairer := NewMockNodeReadRepairer()
                    partitionResolver.defaultPartitionResponse = 500
                    partitionResolver.defaultReplicaNodesResponse = []uint64{ 2, 4, 6 }
                    nodeClient.getCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
                        switch nodeID {
                        case 2:
                            return []*SiblingSet{ siblingSet1, siblingSet2, nil }, nil
                        case 4:
                            return nil, errors.New("Cancelled")
                        case 6:
                            return nil, errors.New("Cancelled")
                        }

                        return nil, nil
                    }

                    var callStartTime time.Time

                    beginRepairCalled := make(chan int, 1)
                    nodeReadRepairer.beginRepairCB = func(readMerger NodeReadMerger) {
                        defer GinkgoRecover()

                        Expect(time.Since(callStartTime) < time.Millisecond * 100).Should(BeTrue())
                        Expect(readMerger.Get("a")).Should(Equal(siblingSet1))
                        Expect(readMerger.Get("b")).Should(Equal(siblingSet2))
                        Expect(readMerger.Get("c")).Should(BeNil())

                        beginRepairCalled <- 1
                    }

                    agent := NewAgent()
                    agent.PartitionResolver = partitionResolver
                    agent.NodeClient = nodeClient
                    agent.NodeReadRepairer = nodeReadRepairer
                    agent.Timeout = time.Second // deadline is one second

                    getReturned := make(chan int)

                    go func() {
                        defer GinkgoRecover()

                        callStartTime = time.Now()
                        siblingSets, err := agent.Get(context.TODO(), "site1", "default", [][]byte{ []byte("a"), []byte("b"), []byte("c") })

                        Expect(siblingSets).Should(BeNil())
                        Expect(err).Should(Equal(ENoQuorum))
                        Expect(time.Since(callStartTime) < time.Millisecond * 100).Should(BeTrue())

                        getReturned <- 1
                    }()

                    select {
                    case <-getReturned:
                    case <-time.After(time.Second):
                        Fail("Get didn't return in time")
                    }

                    select {
                    case <-beginRepairCalled:
                    case <-time.After(time.Second * 2):
                        Fail("BeginRepair wasn't called")
                    }
                })
            })
        })
    })

    Describe("#GetMatches", func() {
    })
})
