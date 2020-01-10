package transfer_test
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
    "time"

    . "github.com/armPelionEdge/devicedb/transfer"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("TransferAgent", func() {
    Describe("HTTPTransferAgent", func() {
        Describe("#StartTransfer", func() {
            It("Should create a partition replica transfer proposal and pass in the done channel of the latest download request", func() {
                downloader := NewMockPartitionDownloader()
                transferProposer := NewMockPartitionTransferProposer()
                transferAgent := NewHTTPTransferAgent(nil, transferProposer, downloader, nil, nil)
                downloadCalled := make(chan int)
                queueTransferProposalCalled := make(chan int)

                downloader.onDownload(func(partition uint64) {
                    Expect(partition).Should(Equal(uint64(1)))

                    downloadCalled <- 1
                })

                transferProposer.onQueueTransferProposal(func(partition uint64, replica uint64, after <-chan int) {
                    Expect(partition).Should(Equal(uint64(1)))
                    Expect(replica).Should(Equal(uint64(1)))

                    queueTransferProposalCalled <- 1
                })

                go transferAgent.StartTransfer(1, 1)

                select {
                case <-downloadCalled:
                case <-time.After(time.Second):
                    Fail("Test timed out")
                }

                select {
                case <-queueTransferProposalCalled:
                case <-time.After(time.Second):
                    Fail("Test timed out")
                }
            })
        })

        Describe("#StopTransfer", func() {
            Context("There is more than one transfer queued for the specified partition", func() {
                It("Should keep the download for that partition running after cancelling the transfer", func() {
                    downloader := NewMockPartitionDownloader()
                    transferProposer := NewMockPartitionTransferProposer()
                    transferAgent := NewHTTPTransferAgent(nil, transferProposer, downloader, nil, nil)
                    cancelDownloadCalled := make(chan int)

                    downloader.onCancelDownload(func(partition uint64) {
                        Expect(partition).Should(Equal(uint64(22)))

                        cancelDownloadCalled <- 1
                    })

                    transferAgent.StartTransfer(22, 1)
                    transferAgent.StartTransfer(22, 2)

                    transferAgent.StopTransfer(22, 1)

                    select {
                    case <-cancelDownloadCalled:
                        Fail("The download should not have been cancelled")
                    case <-time.After(time.Second):
                    }
                })
            })

            Context("There is exactly one transfer queued for the specified partition", func() {
                It("Should cancel the download for that partition after cancelling the transfer", func() {
                    downloader := NewMockPartitionDownloader()
                    transferProposer := NewMockPartitionTransferProposer()
                    transferAgent := NewHTTPTransferAgent(nil, transferProposer, downloader, nil, nil)
                    cancelDownloadCalled := make(chan int)

                    downloader.onCancelDownload(func(partition uint64) {
                        Expect(partition).Should(Equal(uint64(22)))

                        cancelDownloadCalled <- 1
                    })

                    transferAgent.StartTransfer(22, 1)
                    go transferAgent.StopTransfer(22, 1)

                    select {
                    case <-cancelDownloadCalled:
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }
                })
            })
        })

        Describe("#StopAllTransfers", func() {
            It("Should stop all transfers and all downloads", func() {
                downloader := NewMockPartitionDownloader()
                transferProposer := NewMockPartitionTransferProposer()
                transferAgent := NewHTTPTransferAgent(nil, transferProposer, downloader, nil, nil)
                cancelDownloadCalled := make(chan uint64)

                downloader.onCancelDownload(func(partition uint64) {
                    cancelDownloadCalled <- partition
                })

                transferAgent.StartTransfer(22, 1)
                transferAgent.StartTransfer(22, 2)
                transferAgent.StartTransfer(22, 3)
                transferAgent.StartTransfer(23, 1)
                transferAgent.StartTransfer(23, 2)
                transferAgent.StartTransfer(23, 3)
                go transferAgent.StopAllTransfers()

                expectedDownloadCancels := map[uint64]bool{
                    22: true,
                    23: true,
                }

                for {
                    select {
                    case partition := <-cancelDownloadCalled:
                        _, ok := expectedDownloadCancels[partition]

                        Expect(ok).Should(BeTrue())
                        delete(expectedDownloadCancels, partition)

                        if len(expectedDownloadCancels) == 0 {
                            return
                        }
                    case <-time.After(time.Second):
                        Fail("Test timed out")
                    }
                }
            })
        })
    })
})
