// This module bridges the gap between the cluster configuration controller
// and the raft library
package cluster

import (
    "devicedb/raft"
    . "devicedb/logging"
    . "devicedb/util"

    raftEtc "github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"

    "context"
    "errors"
    "sync"
)

var EBadContext = errors.New("The node addition or removal had an invalid context")
var ERaftNodeStartup = errors.New("Encountered an error while starting up raft controller")
var ERaftProtocolError = errors.New("Raft controller encountered a protocol error")
var ECancelled = errors.New("The request was cancelled")
var EStopped = errors.New("The server was stopped")

type proposalResponse struct {
    err error
}

type ConfigController struct {
    raftNode *raft.RaftNode
    raftTransport *raft.TransportHub
    clusterController *ClusterController
    requestMap *RequestMap
    stop chan int
    restartLock sync.Mutex
}

func NewConfigController(raftNode *raft.RaftNode, raftTransport *raft.TransportHub, clusterController *ClusterController) *ConfigController {
    return &ConfigController{
        raftNode: raftNode,
        raftTransport: raftTransport,
        clusterController: clusterController,
        requestMap: NewRequestMap(),
    }
}

func (cc *ConfigController) AddNode(ctx context.Context, nodeConfig NodeConfig) error {
    encodedAddCommandBody, _ := EncodeClusterCommandBody(ClusterAddNodeBody{ NodeID: nodeConfig.Address.NodeID, NodeConfig: nodeConfig })
    addCommand := ClusterCommand{ Type: ClusterAddNode, Data: encodedAddCommandBody, SubmitterID: cc.clusterController.LocalNodeID, CommandID: cc.nextCommandID() }
    context, _ := EncodeClusterCommand(addCommand)

    respCh := cc.requestMap.MakeRequest(addCommand.CommandID)

    if err := cc.raftNode.AddNode(ctx, nodeConfig.Address.NodeID, context); err != nil {
        cc.requestMap.Respond(addCommand.CommandID, nil)
        return err
    }

    select {
    case resp := <-respCh:
        return resp.(proposalResponse).err
    case <-ctx.Done():
        cc.requestMap.Respond(addCommand.CommandID, nil)
        return ECancelled
    case <-cc.stop:
        cc.requestMap.Respond(addCommand.CommandID, nil)
        return EStopped
    }
}

func (cc *ConfigController) ReplaceNode(ctx context.Context, replacedNodeID uint64, replacementNodeID uint64) error {
    encodedRemoveCommandBody, _ := EncodeClusterCommandBody(ClusterRemoveNodeBody{ NodeID: replacedNodeID, ReplacementNodeID: replacementNodeID })
    replaceCommand := ClusterCommand{ Type: ClusterRemoveNode, Data: encodedRemoveCommandBody, SubmitterID: cc.clusterController.LocalNodeID, CommandID: cc.nextCommandID() }
    context, _ := EncodeClusterCommand(replaceCommand)

    respCh := cc.requestMap.MakeRequest(replaceCommand.CommandID)

    if err := cc.raftNode.RemoveNode(ctx, replacedNodeID, context); err != nil {
        cc.requestMap.Respond(replaceCommand.CommandID, nil)
        return err
    }

    select {
    case resp := <-respCh:
        return resp.(proposalResponse).err
    case <-ctx.Done():
        cc.requestMap.Respond(replaceCommand.CommandID, nil)
        return ECancelled
    case <-cc.stop:
        cc.requestMap.Respond(replaceCommand.CommandID, nil)
        return EStopped
    }
}

func (cc *ConfigController) RemoveNode(ctx context.Context, nodeID uint64) error {
    encodedRemoveCommandBody, _ := EncodeClusterCommandBody(ClusterRemoveNodeBody{ NodeID: nodeID, ReplacementNodeID: 0 })
    removeCommand := ClusterCommand{ Type: ClusterRemoveNode, Data: encodedRemoveCommandBody, SubmitterID: cc.clusterController.LocalNodeID, CommandID: cc.nextCommandID() }
    context, _ := EncodeClusterCommand(removeCommand)

    respCh := cc.requestMap.MakeRequest(removeCommand.CommandID)

    if err := cc.raftNode.RemoveNode(ctx, nodeID, context); err != nil {
        cc.requestMap.Respond(removeCommand.CommandID, nil)
        return err
    }

    select {
    case resp := <-respCh:
        return resp.(proposalResponse).err
    case <-ctx.Done():
        cc.requestMap.Respond(removeCommand.CommandID, nil)
        return ECancelled
    case <-cc.stop:
        cc.requestMap.Respond(removeCommand.CommandID, nil)
        return EStopped
    }
}

func (cc *ConfigController) ClusterCommand(ctx context.Context, commandBody interface{}) error {
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

    encodedCommandBody, _ := EncodeClusterCommandBody(commandBody)
    command.Data = encodedCommandBody
    command.SubmitterID = cc.clusterController.LocalNodeID
    command.CommandID = cc.nextCommandID()
    encodedCommand, _ := EncodeClusterCommand(command)

    respCh := cc.requestMap.MakeRequest(command.CommandID)

    if err := cc.raftNode.Propose(ctx, encodedCommand); err != nil {
        cc.requestMap.Respond(command.CommandID, nil)
        return err
    }

    select {
    case resp := <-respCh:
        return resp.(proposalResponse).err
    case <-ctx.Done():
        cc.requestMap.Respond(command.CommandID, nil)
        return ECancelled
    case <-cc.stop:
        cc.requestMap.Respond(command.CommandID, nil)
        return EStopped
    }
}

func (cc *ConfigController) Start() error {
    cc.restartLock.Lock()
    restored := make(chan int, 1)
    cc.stop = make(chan int)
    cc.restartLock.Unlock()

    cc.raftTransport.OnReceive(func(ctx context.Context, msg raftpb.Message) error {
        return cc.raftNode.Receive(ctx, msg)
    })

    cc.raftNode.OnMessages(func(messages []raftpb.Message) error {
        var wg sync.WaitGroup

        for _, message := range messages {
            wg.Add(1)

            go func(msg raftpb.Message) {
                err := cc.raftTransport.Send(context.TODO(), msg, false)

                if err != nil {
                    if msg.Type == raftpb.MsgSnap {
                        cc.raftNode.ReportSnapshot(msg.To, raftEtc.SnapshotFailure)
                    } else {
                        cc.raftNode.ReportUnreachable(msg.To)
                    }
                } else if msg.Type == raftpb.MsgSnap {
                    cc.raftNode.ReportSnapshot(msg.To, raftEtc.SnapshotFinish)
                }

                wg.Done()
            }(message)
        }

        wg.Wait()

        return nil
    })

    cc.raftNode.OnSnapshot(func(snap raftpb.Snapshot) error {
        // but this wont alert to any token gains or anything
        return cc.clusterController.State.Recover(snap.Data)
    })

    cc.raftNode.OnCommittedEntry(func(entry raftpb.Entry) error {
        Log.Debugf("New entry [%d]: %v", entry.Index, entry)

        var encodedClusterCommand []byte

        switch entry.Type {
        case raftpb.EntryConfChange:
            var confChange raftpb.ConfChange

            if err := confChange.Unmarshal(entry.Data); err != nil {
                return err
            }

            clusterCommand, err := DecodeClusterCommand(confChange.Context)

            if err != nil {
                return err
            }

            clusterCommandBody, err := DecodeClusterCommandBody(clusterCommand)

            if err != nil {
                return err
            }

            switch clusterCommand.Type {
            case ClusterAddNode:
                if clusterCommandBody.(ClusterAddNodeBody).NodeID != clusterCommandBody.(ClusterAddNodeBody).NodeConfig.Address.NodeID {
                    return EBadContext
                }

                cc.raftTransport.AddPeer(clusterCommandBody.(ClusterAddNodeBody).NodeConfig.Address)
            case ClusterRemoveNode:
                cc.raftTransport.RemovePeer(raft.PeerAddress{ NodeID: clusterCommandBody.(ClusterRemoveNodeBody).NodeID })
            default:
                return EBadContext
            }

            encodedClusterCommand = confChange.Context
        case raftpb.EntryNormal:
            encodedClusterCommand = entry.Data
        }

        clusterCommand, err := DecodeClusterCommand(encodedClusterCommand)

        if err != nil {
            return err
        }

        if err := cc.clusterController.Step(clusterCommand); err != nil {
            if clusterCommand.SubmitterID == cc.clusterController.LocalNodeID {
                cc.requestMap.Respond(clusterCommand.CommandID, proposalResponse{
                    err: err,
                })
            }

            return err
        }

        if clusterCommand.SubmitterID == cc.clusterController.LocalNodeID {
            cc.requestMap.Respond(clusterCommand.CommandID, proposalResponse{
                err: nil,
            })
        }

        return nil
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
    cc.restartLock.Lock()
    defer cc.restartLock.Unlock()

    if cc.stop == nil {
        return
    }

    cc.raftNode.Stop()
    close(cc.stop)
    cc.stop = nil
}

func (cc *ConfigController) nextCommandID() uint64 {
    return UUID64()
}