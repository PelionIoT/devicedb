package clusterio_test

import (
    "context"
    "sync"
    "time"

    . "devicedb/clusterio"
    . "devicedb/data"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("ReadRepairer", func() {
    Describe("#BeginRepair", func() {
        Context("When the read repairer has not been stopped", func() {
            It("Should call NodeClient.Merge() for each node involved in the read merger", func() {
                nodeClient := NewMockNodeClient()
                readRepairer := NewReadRepairer(nodeClient)
                var nodeSetMutex sync.Mutex
                nodeSet := map[uint64]bool{ 2: true, 4: true, 6: true }
                mergeCalled := make(chan int, 3)
                nodeClient.mergeCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error {
                    defer GinkgoRecover()

                    nodeSetMutex.Lock()
                    defer nodeSetMutex.Unlock()
                    _, ok := nodeSet[nodeID]
                    Expect(ok).Should(BeTrue())
                    Expect(partition).Should(Equal(uint64(50)))
                    Expect(siteID).Should(Equal("site1"))
                    Expect(bucket).Should(Equal("default"))
                    Expect(broadcastToRelays).Should(BeTrue())
                    delete(nodeSet, nodeID)

                    mergeCalled <- 1

                    return nil
                }

                readMerger := NewReadMerger()
                readMerger.InsertKeyReplica(2, "a", nil)
                readMerger.InsertKeyReplica(4, "b", nil)
                readMerger.InsertKeyReplica(6, "c", nil)

                readRepairer.BeginRepair(50, "site1", "default", readMerger)

                for i := 0; i < 3; i++ {
                    select {
                    case <-mergeCalled:
                    case <-time.After(time.Second):
                        Fail("Should have called NodeClient.Merge()")
                    }
                }
            })

            It("The context passed into NodeClient.Merge() should have a deadline as specified by the Timeout parameter of the ReadRepairer", func() {
                var timeout time.Duration = time.Second * 2

                nodeClient := NewMockNodeClient()
                readRepairer := NewReadRepairer(nodeClient)
                readRepairer.Timeout = timeout
                mergeCalled := make(chan int, 3)
                nodeClient.mergeCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error {
                    defer GinkgoRecover()

                    select {
                    case <-ctx.Done():
                    case <-time.After(timeout + time.Millisecond * 100):
                        Fail("Should have cancelled at the deadline")
                    }

                    mergeCalled <- 1

                    return nil
                }

                readMerger := NewReadMerger()
                readMerger.InsertKeyReplica(2, "a", nil)
                readMerger.InsertKeyReplica(4, "b", nil)
                readMerger.InsertKeyReplica(6, "c", nil)

                readRepairer.BeginRepair(50, "site1", "default", readMerger)

                for i := 0; i < 3; i++ {
                    select {
                    case <-mergeCalled:
                    case <-time.After(timeout + time.Second):
                        Fail("Should have called NodeClient.Merge()")
                    }
                }
            })
        })

        Context("When the read repairer has been stopped", func() {
            It("Should abort the operation and not call NodeClient.Merge() at all", func() {
                nodeClient := NewMockNodeClient()
                readRepairer := NewReadRepairer(nodeClient)
                readRepairer.Timeout = time.Second
                mergeCalled := make(chan int, 3)
                nodeClient.mergeCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error {
                    mergeCalled <- 1

                    return nil
                }

                readMerger := NewReadMerger()
                readMerger.InsertKeyReplica(2, "a", nil)
                readMerger.InsertKeyReplica(4, "b", nil)
                readMerger.InsertKeyReplica(6, "c", nil)

                readRepairer.StopRepairs()
                readRepairer.BeginRepair(50, "site1", "default", readMerger)

                select {
                case <-mergeCalled:
                    Fail("Should not have called merge")
                case <-time.After(time.Second * 2):
                }
            })
        })
    })

    Describe("#StopRepairs", func() {
        It("Should cancel any pending calls to NodeClient.Merge() by cancelling the associated context", func() {
            var timeout time.Duration = time.Minute

            nodeClient := NewMockNodeClient()
            readRepairer := NewReadRepairer(nodeClient)
            readRepairer.Timeout = timeout
            mergeCalled := make(chan int, 3)
            nodeClient.mergeCB = func(ctx context.Context, nodeID uint64, partition uint64, siteID string, bucket string, patch map[string]*SiblingSet, broadcastToRelays bool) error {
                defer GinkgoRecover()

                select {
                case <-ctx.Done():
                case <-time.After(time.Second * 2):
                    Fail("Should have been cancelled at one second")
                }

                mergeCalled <- 1

                return nil
            }

            readMerger := NewReadMerger()
            readMerger.InsertKeyReplica(2, "a", nil)
            readMerger.InsertKeyReplica(4, "b", nil)
            readMerger.InsertKeyReplica(6, "c", nil)

            readRepairer.BeginRepair(50, "site1", "default", readMerger)

            <-time.After(time.Second)
            
            readRepairer.StopRepairs()

            for i := 0; i < 3; i++ {
                select {
                case <-mergeCalled:
                case <-time.After(timeout + time.Second):
                    Fail("Should have called NodeClient.Merge()")
                }
            }
        })
    })
})
