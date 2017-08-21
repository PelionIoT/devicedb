type CloudNodeController struct {

}

func (cnc *CloudNodeController) run() {
    for {
        select {
            case delta := <-cnc.clusterController.LocalUpdates:
                server.StepClusterUpdate(delta)
        }
    }
}

func (cnc *CloudNodeController) StepClusterUpdate(delta ClusterStateDelta) {
    switch delta.Type {
    case DeltaNodeAdd:
    case DeltaNodeRemove:
    case DeltaNodeGainPartitionReplica:
    case DeltaNodeLosePartitionReplica:
    case DeltaNodeGainToken:
    case DeltaNodeLoseToken:
    }
}