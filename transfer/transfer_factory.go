package transfer

import (
    "io"
    "time"

    . "devicedb/data"
    . "devicedb/partition"

    "github.com/gorilla/mux"
)

type PartitionTransferFactory interface {
    CreateIncomingTransfer(reader io.Reader) PartitionTransfer
    CreateOutgoingTransfer(nodeID uint64, partition uint64) (PartitionTransfer, error)
}
