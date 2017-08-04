// This module bridges the gap between the cluster configuration controller
// and the raft library
package cloud

import (
    "devicedb/cloud/cluster"
    "devicedb/cloud/raft"

    "github.com/coreos/etcd/raft/raftpb"

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
}

func NewConfigController(raftNode *raft.RaftNode, clusterController *cluster.ClusterController) *ConfigController {
    return &ConfigController{
        raftNode: raftNode,
        clusterController: clusterController,
    }
}

func (cc *ConfigController) ProposeAddNode(ctx context.Context, nodeConfig cluster.NodeConfig) error {
    context, err := cluster.EncodeClusterCommandBody(cluster.ClusterAddNodeBody{ NodeID: nodeConfig.Address.NodeID, NodeConfig: nodeConfig })

    if err != nil {
        return err
    }

    return cc.raftNode.AddNode(ctx, nodeConfig.Address.NodeID, context)
}

func (cc *ConfigController) ProposeRemoveNode(ctx context.Context, nodeID uint64) error {
    return cc.raftNode.RemoveNode(ctx, nodeID)
}

func (cc *ConfigController) ProposeClusterCommand(ctx context.Context, commandBody interface{}) error {
    var command cluster.ClusterCommand = cluster.ClusterCommand{
        SubmitterID: cc.clusterController.LocalNodeID,
    }

    switch commandBody.(type) {
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
    restored := make(chan int, 1)

    cc.raftNode.OnSnapshot(func(snap raftpb.Snapshot) error {
        // but this wont alert to any token gains or anything
        return cc.clusterController.State.Recover(snap.Data)
    })

    cc.raftNode.OnCommittedEntry(func(entry raftpb.Entry) error {
        devicedb.Log.Debugf("New entry [%d]: %v", entry.Index, entry)

        var clusterCommand cluster.ClusterCommand

        switch entry.Type {
        case raftpb.EntryConfChange:
            var confChange raftpb.ConfChange

            if err := confChange.Unmarshal(entry.Data); err != nil {
                return err
            }

            switch confChange.Type {
            case raftpb.ConfChangeAddNode:
                commandBody, err := cluster.DecodeClusterCommandBody(cluster.ClusterCommand{ Type: cluster.ClusterAddNode, Data: confChange.Context })

                if err != nil {
                    return err
                }

                clusterCommand, err = cluster.CreateClusterCommand(cluster.ClusterAddNode, commandBody)

                if err != nil {
                    return err
                }
            case raftpb.ConfChangeRemoveNode:
                clusterCommandBody, err := cluster.EncodeClusterCommandBody(cluster.ClusterRemoveNodeBody{ NodeID: confChange.NodeID })

                if err != nil {
                    return err
                }

                clusterCommand = cluster.ClusterCommand{ Type: cluster.ClusterRemoveNode, Data: clusterCommandBody }

                if err != nil {
                    return err
                }
            }
        case raftpb.EntryNormal:
            var err error
            clusterCommand, err = cluster.DecodeClusterCommand(entry.Data)

            if err != nil {
                return err
            }
        }

        return cc.clusterController.Step(clusterCommand)
    })

    cc.raftNode.OnError(func(err error) error {
        // indicates that raft node is shutting down
        devicedb.Log.Criticalf("Raft node encountered an unrecoverable error and will now shut down: %v", err)

        return nil
    })

    cc.raftNode.OnReplayDone(func() error {
        devicedb.Log.Debug("OnReplayDone() called")
        restored <- 1

        return nil
    })

    if err := cc.raftNode.Start(); err != nil {
        devicedb.Log.Criticalf("Unable to start the config controller due to an error while starting up raft node: %v", err.Error())

        return ERaftNodeStartup
    }

    devicedb.Log.Info("Config controller started up raft node. It is now waiting for log replay...")

    <-restored

    devicedb.Log.Info("Config controller log replay complete")
    return nil
}

func (cc *ConfigController) Stop() {
    cc.raftNode.Stop()
}