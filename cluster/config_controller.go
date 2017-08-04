// This module bridges the gap between the cluster configuration controller
// and the raft library
package cluster

import (
    "devicedb/raft"
    . "devicedb/logging"

    "github.com/coreos/etcd/raft/raftpb"

    "encoding/json"
    "context"
    "errors"
)

var ERaftNodeStartup = errors.New("Encountered an error while starting up raft controller")
var ERaftProtocolError = errors.New("Raft controller encountered a protocol error")

type ConfigController struct {
    raftNode *raft.RaftNode
    clusterController *ClusterController
}

func NewConfigController(raftNode *raft.RaftNode, clusterController *ClusterController) *ConfigController {
    return &ConfigController{
        raftNode: raftNode,
        clusterController: clusterController,
    }
}

func (cc *ConfigController) ProposeAddNode(ctx context.Context, nodeConfig NodeConfig) error {
    context, err := EncodeClusterCommandBody(ClusterAddNodeBody{ NodeID: nodeConfig.Address.NodeID, NodeConfig: nodeConfig })

    if err != nil {
        return err
    }

    return cc.raftNode.AddNode(ctx, nodeConfig.Address.NodeID, context)
}

func (cc *ConfigController) ProposeRemoveNode(ctx context.Context, nodeID uint64) error {
    return cc.raftNode.RemoveNode(ctx, nodeID)
}

func (cc *ConfigController) ProposeClusterCommand(ctx context.Context, commandBody interface{}) error {
    var command ClusterCommand = ClusterCommand{
        SubmitterID: cc.clusterController.LocalNodeID,
    }

    switch commandBody.(type) {
    case ClusterUpdateNodeBody:
        command.Type = ClusterUpdateNode
    case ClusterTakePartitionReplicaBody:
        command.Type = ClusterTakePartitionReplica
    case ClusterSetReplicationFactorBody:
        command.Type = ClusterSetReplicationFactor
    case ClusterSetPartitionCountBody:
        command.Type = ClusterSetPartitionCount
    default:
        return ENoSuchCommand
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
        Log.Debugf("New entry [%d]: %v", entry.Index, entry)

        var clusterCommand ClusterCommand

        switch entry.Type {
        case raftpb.EntryConfChange:
            var confChange raftpb.ConfChange

            if err := confChange.Unmarshal(entry.Data); err != nil {
                return err
            }

            switch confChange.Type {
            case raftpb.ConfChangeAddNode:
                commandBody, err := DecodeClusterCommandBody(ClusterCommand{ Type: ClusterAddNode, Data: confChange.Context })

                if err != nil {
                    return err
                }

                clusterCommand, err = CreateClusterCommand(ClusterAddNode, commandBody)

                if err != nil {
                    return err
                }
            case raftpb.ConfChangeRemoveNode:
                clusterCommandBody, err := EncodeClusterCommandBody(ClusterRemoveNodeBody{ NodeID: confChange.NodeID })

                if err != nil {
                    return err
                }

                clusterCommand = ClusterCommand{ Type: ClusterRemoveNode, Data: clusterCommandBody }

                if err != nil {
                    return err
                }
            }
        case raftpb.EntryNormal:
            var err error
            clusterCommand, err = DecodeClusterCommand(entry.Data)

            if err != nil {
                return err
            }
        }

        return cc.clusterController.Step(clusterCommand)
    })

    cc.raftNode.OnError(func(err error) error {
        // indicates that raft node is shutting down
        Log.Criticalf("Raft node encountered an unrecoverable error and will now shut down: %v", err)

        return nil
    })

    cc.raftNode.OnReplayDone(func() error {
        Log.Debug("OnReplayDone() called")
        restored <- 1

        return nil
    })

    if err := cc.raftNode.Start(); err != nil {
        Log.Criticalf("Unable to start the config controller due to an error while starting up raft node: %v", err.Error())

        return ERaftNodeStartup
    }

    Log.Info("Config controller started up raft node. It is now waiting for log replay...")

    <-restored

    Log.Info("Config controller log replay complete")
    return nil
}

func (cc *ConfigController) Stop() {
    cc.raftNode.Stop()
}