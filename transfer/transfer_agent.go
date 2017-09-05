package transfer

import (
    "io"
    "sync"
    "time"

    . "devicedb/data"
    . "devicedb/cluster"
    . "devicedb/partition"
    . "devicedb/util"

    "github.com/gorilla/mux"
)

type TransferAgent interface {
    StartTransfer(partition PartitionReplica)
    StopTransfer(partition PartitionReplica)
    Stop()
}

type HTTPTransferAgent struct {
    transferFactory PartitionTransferFactory
    configController *ConfigController
}

func NewHTTPTransferAgent(transferFactory PartitionTransferFactory, configController *ConfigController) *HTTPTransferAgent {
    return &HTTPTransferAgent{
        transferFactory: transferFactory,
        configController: configController,
    }
}

func (transferAgent *HTTPTransferAgent) StartTransfer(partition PartitionReplica) {
}

func (transferAgent *HTTPTransferAgent) StopTransfer(partition PartitionReplica) {
}

func (transferAgent *HTTPTransferAgent) Stop() {
}

func (transferAgent *HTTPTransferAgent) Attach(router mux.Router) {
}