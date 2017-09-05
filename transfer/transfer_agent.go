package transfer

import (
    "net/http"
    "io"
    "sync"
    "time"
    "context"

    . "devicedb/cluster"
    . "devicedb/logging"
    . "devicedb/partition"
)

const RetryTimeoutMax = 32

type PartitionTransferAgent interface {
    StartTransfer(partition uint64, replica uint64)
    StopTransfer(partition uint64, replica uint64)
    Stop()
}

type HTTPTransferAgent struct {
    transferFactory PartitionTransferFactory
    configController *ConfigController
    partitionPool PartitionPool
    transferPartnerStrategy PartitionTransferPartnerStrategy
    transferProposer PartitionTransferProposer
    transferTransport PartitionTransferTransport
    currentDownloads map[uint64]chan int
    downloadCancelers map[uint64]chan int
    transferCancelers map[uint64]map[uint64]chan int
    lock sync.Mutex
}

func NewHTTPTransferAgent(transferFactory PartitionTransferFactory, partitionPool PartitionPool, transferPartnerStrategy PartitionTransferPartnerStrategy, transferProposer PartitionTransferProposer, transport PartitionTransferTransport, configController *ConfigController) *HTTPTransferAgent {
    return &HTTPTransferAgent{
        transferTransport: transport,
        transferFactory: transferFactory,
        partitionPool: partitionPool,
        configController: configController,
        transferPartnerStrategy: transferPartnerStrategy,
        transferProposer: transferProposer,
    }
}

func (transferAgent *HTTPTransferAgent) StartTransfer(partition uint64, replica uint64) {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    transferCancel := make(chan int, 1)

    if _, ok := transferAgent.transferCancelers[partition]; !ok {
        transferAgent.transferCancelers[partition] = make(map[uint64]chan int, 0)
    }

    transferAgent.transferCancelers[partition][replica] = transferCancel
    transferAgent.proposeTransferAfter(partition, replica, transferCancel, transferAgent.downloadPartition(partition))
}

func (transferAgent *HTTPTransferAgent) proposeTransferAfter(partition uint64, replica uint64, cancel chan int, after chan int) {
    go func() {
        defer func() {
            transferAgent.lock.Lock()
            defer transferAgent.lock.Unlock()

            // Delete any cancels from any maps
            if _, ok := transferAgent.transferCancelers[partition]; !ok {
                return
            }

            // It is possible that this proposal was cancelled but then one for the
            // same replica was started before this cleanup function was called. In
            // this case the map might contain a new canceller for a new proposal for
            // the same replica. This requires an equality check so this proposal doesn't
            // step on the toes of another one
            if transferAgent.transferCancelers[partition][replica] == cancel {
                delete(transferAgent.transferCancelers[partition], replica)
            }
        }()

        if after != nil {
            select {
            case <-after:
            case <-cancel:
                return
            }
        }

        ctx, ctxCancel := context.WithCancel(context.Background())

        go func() {
            select {
            case <-ctx.Done():
            case <-cancel:
                ctxCancel()
            }
        }()

        transferAgent.transferProposer.ProposeTransfer(ctx, partition, replica)
    }()
}

// If this function results in a new download occurring it should return a channel that closes when
// the download is complete. If a download already exists for this partition then return the after
// channel for that download. If there is not currently a download for this partition and there should
// be no new one since it was already downloaded successfully in the past then return a nil channel
// to indicate there is nothing to wait for before proposing a replica transfer
func (transferAgent *HTTPTransferAgent) downloadPartition(partition uint64) chan int {
    node := transferAgent.configController.ClusterController().State.Nodes[transferAgent.configController.ClusterController().LocalNodeID]

    if _, ok := node.PartitionReplicas[partition]; ok {
        return nil
    }

    if _, ok := transferAgent.currentDownloads[partition]; ok {
        return transferAgent.currentDownloads[partition]
    }

    cancel := make(chan int, 1)
    done := make(chan int)

    transferAgent.downloadCancelers[partition] = cancel
    transferAgent.currentDownloads[partition] = done

    go func() {
        defer func() {
            transferAgent.lock.Lock()
            defer transferAgent.lock.Unlock()

            if _, ok := transferAgent.downloadCancelers[partition]; !ok {
                return
            }

            if transferAgent.downloadCancelers[partition] == cancel {
                delete(transferAgent.downloadCancelers, partition)
                delete(transferAgent.currentDownloads, partition)
            }
        }()

        retryTimeoutSeconds := 0

        Log.Infof("Local node (id = %d) starting transfer to obtain a replica of partition %d", transferAgent.configController.ClusterController().LocalNodeID, partition)

        for {
            if retryTimeoutSeconds != 0 {
                Log.Infof("Local node (id = %d) will attempt to obtain a replica of partition %d again in %d seconds", transferAgent.configController.ClusterController().LocalNodeID, partition, retryTimeoutSeconds)

                select {
                    case <-time.After(time.Second * time.Duration(retryTimeoutSeconds)):
                    case <-cancel:
                        Log.Infof("Local node (id = %d) cancelled all transfers for partition %d. Cancelling download.", transferAgent.configController.ClusterController().LocalNodeID, partition)
                        return
                }
            }

            partnerID := transferAgent.transferPartnerStrategy.ChooseTransferPartner(partition)

            if partnerID == 0 {
                // No other node holds a replica of this partition. Move onto the phase where we propose
                // a transfer in the raft log
                break
            }

            Log.Infof("Local node (id = %d) starting transfer of partition %d from node %d", transferAgent.configController.ClusterController().LocalNodeID, partition, partnerID)
            reader, closeReader, err := transferAgent.transferTransport.Get(partnerID, partition)

            if err != nil {
                Log.Warningf("Local node (id = %d) unable to obtain a replica of partition %d from node %d: %v", transferAgent.configController.ClusterController().LocalNodeID, partition, partnerID, err.Error())
                
                if retryTimeoutSeconds == 0 {
                    retryTimeoutSeconds = 1
                } else if retryTimeoutSeconds != RetryTimeoutMax {
                    retryTimeoutSeconds *= 2
                }

                continue
            }

            retryTimeoutSeconds = 0
            partitionTransfer := transferAgent.transferFactory.CreateIncomingTransfer(reader)
            chunks := make(chan PartitionChunk)
            errors := make(chan error)

            go func() {
                for {
                    nextChunk, err := partitionTransfer.NextChunk()

                    if !nextChunk.IsEmpty() {
                        chunks <- nextChunk
                    }

                    if err != nil {
                        errors <- err
                        break
                    }
                }

                errors <- nil
            }()

            run := true
            retry := false

            for run {
                select {
                case chunk := <-chunks:
                    // TODO write to partition
                    Log.Infof("%v", chunk)

                case err := <-errors:
                    if err == nil {
                        // indicates that the loop reading chunks from the transfer
                        // has terminated and no more errors will be read from errors
                        run = false
                        break
                    }

                    // If any error occurs other than an indication of the end of the stream
                    // the the transfer needs to be restarted and tried again
                    retry = (err == io.EOF)
                case <-cancel:
                    partitionTransfer.Cancel()
                    closeReader()
                }
            }

            if !retry {
                // The download was successful
                break
            }

            // Need to try again 
            if retryTimeoutSeconds == 0 {
                retryTimeoutSeconds = 1
            } else if retryTimeoutSeconds != RetryTimeoutMax {
                retryTimeoutSeconds *= 2
            }
        }

        // closing done signals to any pending replica transfer
        // proposers that the data transfer has finished and now
        // is time to propose the raft log transfer
        close(done)
    }()

    return done
}

func (transferAgent *HTTPTransferAgent) StopTransfer(partition uint64, replica uint64) {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    transferAgent.stopTransfer(partition, replica)
}

func (transferAgent *HTTPTransferAgent) stopTransfer(partition, replica uint64) {
    if _, ok := transferAgent.transferCancelers[partition]; !ok {
        return
    }

    if cancel, ok := transferAgent.transferCancelers[partition][replica]; ok {
        delete(transferAgent.transferCancelers[partition], replica)

        cancel <- 1
    }

    if len(transferAgent.transferCancelers[partition]) == 0 {
        delete(transferAgent.transferCancelers, partition)

        // If there are no more pending transfers for this partition then
        // any existing download for this partition should be cancelled
        if cancel, ok := transferAgent.downloadCancelers[partition]; ok {
            cancel <- 1

            delete(transferAgent.downloadCancelers, partition)
            delete(transferAgent.currentDownloads, partition)
        }
    }
}

func (transferAgent *HTTPTransferAgent) Stop() {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    for partition, replicas := range transferAgent.transferCancelers {
        for replica, _ := range replicas {
            transferAgent.stopTransfer(partition, replica)
        }
    }
}

func (transferAgent *HTTPTransferAgent) Handler() func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, req *http.Request) {

    }
}
