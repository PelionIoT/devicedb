package transfer

import (
    "context"
    "errors"
    "io"
    "sync"
    "time"

    . "devicedb/cluster"
    . "devicedb/data"
    . "devicedb/logging"
    . "devicedb/partition"
)

type PartitionDownloader interface {
    // Starts the download process for a partition if it is not yet downloaded and
    // there isn't yet a download occurring for that partition.
    // Returns a channel that closes when the download is complete
    // If the download is successful all future calls to Download for that partition
    // should return that closed channel until CancelDownload is called
    // which resets it
    Download(partition uint64) <-chan int
    // Returns a boolean indicating whether or not a download is in progress
    // for this partition
    IsDownloading(partition uint64) bool
    // Cancels any download in progress. Resets internal state so next
    // call to Download for a partition will start a new download and
    // return a new after channel
    CancelDownload(partition uint64)
}

type Downloader struct {
    transferTransport PartitionTransferTransport
    transferPartnerStrategy PartitionTransferPartnerStrategy
    transferFactory PartitionTransferFactory
    partitionPool PartitionPool
    configController *ConfigController
    downloadCancelers map[uint64]*Canceler
    currentDownloads map[uint64]chan int
    downloadStopCB func(uint64)
    panicCB func(p interface{})
    lock sync.Mutex
}

func NewDownloader(configController *ConfigController, transferTransport PartitionTransferTransport, transferPartnerStrategy PartitionTransferPartnerStrategy, transferFactory PartitionTransferFactory, partitionPool PartitionPool) *Downloader {
    return &Downloader{
        downloadCancelers: make(map[uint64]*Canceler, 0),
        currentDownloads: make(map[uint64]chan int, 0),
        configController: configController,
        transferTransport: transferTransport,
        transferPartnerStrategy: transferPartnerStrategy,
        transferFactory: transferFactory,
        partitionPool: partitionPool,
    }
}

// A callback that will be invoked after a download for a partition is cancelled
// or completed. Used only for tooling in order to test the flow of the downloader
// code
func (downloader *Downloader) OnDownloadStop(cb func(partition uint64)) {
    downloader.downloadStopCB = cb
}
// A callback that will be invoked if there is a panic that occurs while writing
// keys from a transfer. Used only for tooling in order to test the flow of the downloader
// code
func (downloader *Downloader) OnPanic(cb func(p interface{})) {
    downloader.panicCB = cb
}

func (downloader *Downloader) notifyDownloadStop(partition uint64) {
    if downloader.downloadStopCB != nil {
        downloader.downloadStopCB(partition)
    }
}

func (downloader *Downloader) Download(partition uint64) <-chan int {
    downloader.lock.Lock()
    defer downloader.lock.Unlock()

    // A download is already underway for this partition
    if _, ok := downloader.currentDownloads[partition]; ok {
        return downloader.currentDownloads[partition]
    }

    node := downloader.configController.ClusterController().State.Nodes[downloader.configController.ClusterController().LocalNodeID]
    done := make(chan int)

    // Since this node is already a holder of this partition there is no need to
    // start a download. Just propose any pending transfers straight away
    if _, ok := node.PartitionReplicas[partition]; ok {
        close(done)

        return done
    }

    ctx, cancel := context.WithCancel(context.Background())
    canceler := &Canceler{ Cancel: cancel }
    downloader.downloadCancelers[partition] = canceler
    downloader.currentDownloads[partition] = done

    go func() {
        defer func() {
            downloader.lock.Lock()
            defer downloader.lock.Unlock()

            if _, ok := downloader.downloadCancelers[partition]; !ok {
                return
            }

            if downloader.downloadCancelers[partition] == canceler {
                delete(downloader.downloadCancelers, partition)
            }

            downloader.notifyDownloadStop(partition)

            if r := recover(); r != nil {
                if downloader.panicCB == nil {
                    panic(r)
                }

                downloader.panicCB(r)
            }
        }()

        retryTimeoutSeconds := 0

        Log.Infof("Local node (id = %d) starting transfer to obtain a replica of partition %d", downloader.configController.ClusterController().LocalNodeID, partition)

        for {
            if retryTimeoutSeconds != 0 {
                Log.Infof("Local node (id = %d) will attempt to obtain a replica of partition %d again in %d seconds", downloader.configController.ClusterController().LocalNodeID, partition, retryTimeoutSeconds)

                select {
                    case <-time.After(time.Second * time.Duration(retryTimeoutSeconds)):
                    case <-ctx.Done():
                        Log.Infof("Local node (id = %d) cancelled all transfers for partition %d. Cancelling download.", downloader.configController.ClusterController().LocalNodeID, partition)
                        return
                }
            }

            partnerID := downloader.transferPartnerStrategy.ChooseTransferPartner(partition)

            if partnerID == 0 {
                // No other node holds a replica of this partition. Move onto the phase where we propose
                // a transfer in the raft log
                break
            }

            Log.Infof("Local node (id = %d) starting transfer of partition %d from node %d", downloader.configController.ClusterController().LocalNodeID, partition, partnerID)
            reader, closeReader, err := downloader.transferTransport.Get(partnerID, partition)

            if err != nil {
                Log.Warningf("Local node (id = %d) unable to obtain a replica of partition %d from node %d: %v", downloader.configController.ClusterController().LocalNodeID, partition, partnerID, err.Error())
                
                if retryTimeoutSeconds == 0 {
                    retryTimeoutSeconds = 1
                } else if retryTimeoutSeconds != RetryTimeoutMax {
                    retryTimeoutSeconds *= 2
                }

                continue
            }

            retryTimeoutSeconds = 0
            partitionTransfer := downloader.transferFactory.CreateIncomingTransfer(reader)
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
                    Log.Debugf("Local node (id = %d) received chunk %d of partition %d from node %d", downloader.configController.ClusterController().LocalNodeID, chunk.Index, partition, partnerID)
                   
                    if err := downloader.mergeChunk(partition, chunk); err != nil {
                        run = false
                        retry = true
                        break
                    }
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
                case <-ctx.Done():
                    // The download was cancelled externally
                    partitionTransfer.Cancel()
                }
            }

            closeReader()

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

func (downloader *Downloader) mergeChunk(partition uint64, chunk PartitionChunk) error {
    partitionReplica := downloader.partitionPool.Get(partition)

    if partitionReplica == nil {
        var msg string = 
        "This represents a major flaw in the coordination between the downloader and the partition pool " +
        "and is non-recoverable. This should not happen since a precondition for invoking a download of a partition on a node" +
        "is to have initialized that partition in the partition pool, and a precondition for removing a partition from the partition" +
        "pool is having cancelled any transfers or downloads for that partition."

        Log.Panicf("Local node (id = %d) is trying to download data to partition %d which is not initialized\n\n %s", downloader.configController.ClusterController().LocalNodeID, partition, msg)
        
        return errors.New("Partition is not registered")
    }

    for _, entry := range chunk.Entries {
        site := partitionReplica.Sites().Acquire(entry.Site)

        if site == nil {
            // This represents a case where the nodes disagree with which sites exist. This node is unaware
            // of this site while the partner node thinks this site exists. This means one of two cases is true
            //  1) The site used to exist and was since deleted and the local node is further ahead in the log than the remote node
            //  2) The site was recently created and the remote node is futher ahead in the log than the local node
            // The solution in both cases is to wait and try again later when the two nodes are both caught up in the log
            // If case 1 is true:
            //   The remote node will eventually catch up in the log and will either filter out data from the deleted site while
            //   performing the transfer or delete that data entirely from its local storage
            // If case 2 is true:
            //   The local node will eventually catch up and will accept the entries for this site on the next transfer
            Log.Warningf("Local node (id = %d) is trying to download data to site %s in partition %d and doesn't think that site exists.", downloader.configController.ClusterController().LocalNodeID, entry.Site, partition)
            
            return errors.New("Site does not exist")
        }

        bucket := site.Buckets().Get(entry.Bucket)

        if bucket == nil {
            // Since the bucket names are entirely built in and normalized across nodes this should not happen
            // If it does it represents an unrecoverable error and should be looked into
            Log.Panicf("Local node (id = %d) is trying to download data to bucket %s in site %s in partition %d and that bucket doesn't exist at that site.", downloader.configController.ClusterController().LocalNodeID, entry.Bucket, entry.Site, partition)
            
            return errors.New("Bucket does not exist")
        }

        err := bucket.Merge(map[string]*SiblingSet{ entry.Key: entry.Value })

        if err != nil {
            // A storage error like this probably represents some sort of disk or machine failure and should be reported in a way that stands out
            Log.Criticalf("Local node (id = %d) encountered an error while calling Merge() for key %s in bucket %s in site %s in partition %d: %v", entry.Key, entry.Bucket, entry.Site, partition, err.Error())

            return errors.New("Merge error")
        }
    }

    return nil
}

// Important!
// This should only be called by a transfer agent if all transfer
// proposals waiting for this download have been cancelled first
func (downloader *Downloader) CancelDownload(partition uint64) {
    downloader.lock.Lock()
    defer downloader.lock.Unlock()

    // Cancel current download (if any) for this partition
    if canceler, ok := downloader.downloadCancelers[partition]; ok {
        canceler.Cancel()

        delete(downloader.downloadCancelers, partition)
        delete(downloader.currentDownloads, partition)
    }
}

func (downloader *Downloader) IsDownloading(partition uint64) bool {
    downloader.lock.Lock()
    defer downloader.lock.Unlock()

    _, ok := downloader.downloadCancelers[partition]

    return ok
}