package transfer

import (
    "io"
    "net/http"
    "strconv"
    "sync"

    . "devicedb/cluster"
    . "devicedb/logging"
    . "devicedb/partition"

    "github.com/gorilla/mux"
)

const RetryTimeoutMax = 32

type PartitionTransferAgent interface {
    // Tell the partition transfer agent to start the holdership transfer process for this partition replica
    StartTransfer(partition uint64, replica uint64)
    // Tell the partition transfer agent to stop any holdership transfer processes for this partition replica
    StopTransfer(partition uint64, replica uint64)
    StopAllTransfers()
}

type HTTPTransferAgent struct {
    configController ClusterConfigController
    transferProposer PartitionTransferProposer
    partitionDownloader PartitionDownloader
    transferFactory PartitionTransferFactory
    partitionPool PartitionPool
    lock sync.Mutex
}

func NewHTTPTransferAgent(configController ClusterConfigController, transferProposer PartitionTransferProposer, partitionDownloader PartitionDownloader, transferFactory PartitionTransferFactory, partitionPool PartitionPool) *HTTPTransferAgent {
    return &HTTPTransferAgent{
        configController: configController,
        transferProposer: transferProposer,
        partitionDownloader: partitionDownloader,
        transferFactory: transferFactory,
        partitionPool: partitionPool,
    }
}

func (transferAgent *HTTPTransferAgent) StartTransfer(partition uint64, replica uint64) {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    transferAgent.transferProposer.QueueTransferProposal(partition, replica, transferAgent.partitionDownloader.Download(partition))
}

func (transferAgent *HTTPTransferAgent) StopTransfer(partition uint64, replica uint64) {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    transferAgent.stopTransfer(partition, replica)
}

func (transferAgent *HTTPTransferAgent) stopTransfer(partition, replica uint64) {
    transferAgent.transferProposer.CancelTransferProposal(partition, replica)

    if transferAgent.transferProposer.PendingProposals(partition) == 0 {
        transferAgent.partitionDownloader.CancelDownload(partition)
    }
}

func (transferAgent *HTTPTransferAgent) StopAllTransfers() {
    transferAgent.lock.Lock()
    defer transferAgent.lock.Unlock()

    queuedProposals := transferAgent.transferProposer.QueuedProposals()

    for partition, replicas := range queuedProposals {
        for replica, _ := range replicas {
            transferAgent.stopTransfer(partition, replica)
        }
    }
}

func (transferAgent *HTTPTransferAgent) Handler() func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, req *http.Request) {
        partitionNumber, err := strconv.ParseUint(mux.Vars(req)["partition"], 10, 64)

        if err != nil {
            Log.Warningf("Invalid partition number specified in partition transfer HTTP request. Value cannot be parsed as uint64: %s", mux.Vars(req)["partition"])

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusBadRequest)
            io.WriteString(w, "\n")

            return
        }

        partition := transferAgent.partitionPool.Get(partitionNumber)

        if partition == nil {
            Log.Warningf("The specified partition (%d) does not exist at this node. Unable to fulfill transfer request", partitionNumber)

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusNotFound)
            io.WriteString(w, "\n")

            return
        }

        transfer, _ := transferAgent.transferFactory.CreateOutgoingTransfer(partition)
        transfer.UseFilter(func(entry Entry) bool {
            Log.Debugf("Transfer of partition %d ignoring entry from site %s since that site was removed", partitionNumber, entry.Site)

            return transferAgent.configController.ClusterController().SiteExists(entry.Site)
        })

        transferEncoder := NewTransferEncoder(transfer)
        r, err := transferEncoder.Encode()

        if err != nil {
            Log.Warningf("An error occurred while encoding partition %d. Unable to fulfill transfer request: %v", partitionNumber, err.Error())

            w.Header().Set("Content-Type", "application/json; charset=utf8")
            w.WriteHeader(http.StatusInternalServerError)
            io.WriteString(w, "\n")

            return
        }

        Log.Infof("Start sending partition %d to remote node...", partitionNumber)

        w.Header().Set("Content-Type", "application/json; charset=utf8")
        w.WriteHeader(http.StatusOK)
        written, err := io.Copy(w, r)

        if err != nil {
            Log.Errorf("An error occurred while sending partition %d to requesting node after sending %d bytes: %v", partitionNumber, written, err.Error())

            return
        }

        Log.Infof("Done sending partition %d to remote node. Bytes written: %d", partitionNumber, written)
    }
}
