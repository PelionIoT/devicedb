package cloud

import (
    "devicedb/cloud/cluster"
    "devicedb/cloud/raft"

    "devicedb"
    "encoding/json"
    "context"
    "errors"
)

var ERaftNodeStartup = errors.New("Encountered an error while starting up raft controller")
var ERaftProtocolError = errors.New("Raft controller encountered a protocol error")

type ConfigController struct {
    raftNode *raft.RaftNode
    clusterController *cluster.ClusterController
    done chan int
}

func (cc *ConfigController) ProposeClusterCommand(ctx context.Context, commandBody interface{}) error {
    var command cluster.ClusterCommand = cluster.ClusterCommand{
        SubmitterID: cc.clusterController.LocalNodeID,
    }

    switch commandBody.(type) {
    case cluster.ClusterAddNodeBody:
        command.Type = cluster.ClusterAddNode
    case cluster.ClusterRemoveNodeBody:
        command.Type = cluster.ClusterRemoveNode
    case cluster.ClusterUpdateNodeBody:
        command.Type = cluster.ClusterUpdateNode
    case cluster.ClusterTakePartitionReplicaBody:
        command.Type = cluster.ClusterTakePartitionReplica
    case cluster.ClusterSetReplicationFactorBody:
        command.Type = cluster.ClusterSetReplicationFactor
    case cluster.ClusterSetPartitionCountBody:
        command.Type = cluster.ClusterSetPartitionCount
    default:
        return cluster.ENoSuchCommand
    }

    encodedCommandBody, _ := json.Marshal(commandBody)
    command.Data = encodedCommandBody
    encodedCommand, _ := json.Marshal(command)

    return cc.raftNode.Propose(ctx, encodedCommand)
}

func (cc *ConfigController) Start() error {
    if err := cc.raftNode.Start(); err != nil {
        devicedb.Log.Criticalf("Unable to start the config controller due to an error while starting up raft node: %v", err.Error())

        return ERaftNodeStartup
    }

    for {
        select {
            case entry := <-cc.raftNode.Entries():
            case snap := <-cc.raftNode.Snapshots():
            case err := <-cc.raftNode.Errors():
                devicedb.Log.Criticalf("The raft controller encountered a protocol error: %v", err.Error())
                return ERaftProtocolError
            case <-done:
                devicedb.Log.Infof("Stopping config controller")
                return nil
        }
    }
}