package transfer

import (
    "context"
    
    . "devicedb/cluster"
)

type PartitionTransferProposer interface {
    ProposeTransfer(ctx context.Context, partition uint64, replica uint64)
}

type TransferProposer struct {
    configController *ConfigController
}

func NewTransferProposer(configController *ConfigController) *TransferProposer {
    return &TransferProposer{
        configController: configController,
    }
}

func (transferProposer *TransferProposer) ProposeTransfer(ctx context.Context, partition uint64, replica uint64) error {
    return transferProposer.configController.ClusterCommand(ctx, ClusterTakePartitionReplicaBody{ NodeID: transferProposer.configController.ClusterController().LocalNodeID, Partition: partition, Replica: replica })
}
