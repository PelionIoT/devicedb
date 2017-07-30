package cluster

import (
    "encoding/json"
)

type ClusterController struct {
    State ClusterState
}

func (clusterController *ClusterController) Step(clusterCommand ClusterCommand) {
    switch clusterController.State.State {
    case StateStable:
        clusterController.stepFromStateStable(clusterCommand)
    }
}

func (clusterController *ClusterController) stepFromStateStable(clusterCommand ClusterCommand) {
    if clusterCommand.Type == ClusterRingLock {
        clusterController.State.UpdateLocks = append(clusterController.State.UpdateLocks, clusterCommand.SubmitterID)
    } else if clusterCommand.Type == ClusterRingLock {
        if len(clusterController.State.UpdateLocks) != 0 {
            clusterController.State.UpdateLocks = clusterController.State.UpdateLocks[1:]
        }
    }

    if len(clusterController.State.UpdateLocks) != 0 && clusterController.State.UpdateLocks[0] == clusterController.State.NodeID {
        // TODO something
    }
}

func (clusterController *ClusterController) stepFromStateTakeTokens(clusterCommand ClusterCommand) {
    // Take tokens from other nodes
}

func (clusterController *ClusterController) stepFromStateGiveTokens(clusterCommand ClusterCommand) {
    // 
}

