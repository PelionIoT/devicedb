package cluster_test

import (
    "fmt"
    "encoding/binary"
    "crypto/rand"
    "context"
    "time"
    
    . "devicedb/storage"
    . "devicedb/cluster"
    . "devicedb/raft"

    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

func randomString() string {
    randomBytes := make([]byte, 16)
    rand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%05x%05x", high, low)
}

var _ = Describe("ConfigController", func() {
    Describe("Restarting a node", func() {
        Specify("A node should not start until it has replayed all previous log entries and has restored its config state to what it was before it was stopped", func() {
            // Create Cluster Controller
            clusterController := &ClusterController{
                LocalNodeID: 0x1,
                State: ClusterState{ 
                    Nodes: make(map[uint64]*NodeConfig),
                },
                PartitioningStrategy: &SimplePartitioningStrategy{ },
                LocalUpdates: make(chan ClusterStateDelta),
            }

            addNodeBody, _ := EncodeClusterCommandBody(ClusterAddNodeBody{ NodeID: 0x1, NodeConfig: NodeConfig{ Address: PeerAddress{ NodeID: 0x1 }, Capacity: 1 } })
            addNodeContext, _ := EncodeClusterCommand(ClusterCommand{ Type: ClusterAddNode, Data: addNodeBody })

            // Create New Raft Node
            raftNode := NewRaftNode(&RaftNodeConfig{
                ID: 0x1,
                CreateClusterIfNotExist: true,
                Context: addNodeContext,
                Storage: NewRaftStorage(NewLevelDBStorageDriver("/tmp/testraftstore-" + randomString(), nil)),
                GetSnapshot: func() ([]byte, error) {
                    return clusterController.State.Snapshot()
                },
            })

            transport := NewTransportHub()
            // Pass raft node and cluster controller into new config controller
            configController := NewConfigController(raftNode, transport, clusterController)

            // Start Config Controller
            Expect(configController.Start()).Should(BeNil())
            // Wait for node to be added to single-node cluster
            delta := <-clusterController.LocalUpdates
            Expect(delta.Type).Should(Equal(DeltaNodeAdd))
            Expect(delta.Delta.(NodeAdd).NodeID).Should(Equal(uint64(0x1)))

            // Propose A Few Cluster Config Changes
            Expect(configController.ClusterCommand(context.TODO(), ClusterSetPartitionCountBody{ Partitions: 1024 })).Should(BeNil())

            ownedTokens := make(map[uint64]bool)
            done := make(chan int)
            
            // This needs to run in a sepearte goroutine since ClusterCommand blocks until all notifactions are sent to the local node
            go func() {

                // Wait For Changes To Commit
                for i := uint64(0); i < 1024; i++ {
                    delta := <-clusterController.LocalUpdates
                    Expect(delta.Type).Should(Equal(DeltaNodeGainToken))
                    Expect(delta.Delta.(NodeGainToken).NodeID).Should(Equal(uint64(0x1)))

                    ownedTokens[delta.Delta.(NodeGainToken).Token] = true
                }

                done <- 1
            }()

            Expect(configController.ClusterCommand(context.TODO(), ClusterSetReplicationFactorBody{ ReplicationFactor: 2 })).Should(BeNil())

            <-done
            // Verify changes
            Expect(len(ownedTokens)).Should(Equal(1024))
            Expect(ownedTokens).Should(Equal(clusterController.State.Nodes[1].Tokens))

            // Stop old config controller
            configController.Stop()

            // Give it time to shut down
            <-time.After(time.Second)

            // Create a new cluster and config controller to ensure fresh state. Keep the same raft node
            // to ensure logs are restored from the same persistent storage file
            newClusterController := &ClusterController{
                LocalNodeID: 0x1,
                State: ClusterState{ 
                    Nodes: make(map[uint64]*NodeConfig),
                },
                PartitioningStrategy: &SimplePartitioningStrategy{ },
                LocalUpdates: nil,//make(chan ClusterStateDelta),
            }

            newTransport := NewTransportHub()
            newConfigController := NewConfigController(raftNode, newTransport, newClusterController)
            Expect(newConfigController.Start()).Should(BeNil())

            // Make Sure Current Configuration == Configuration Before Controller Was Stopped
            Expect(newClusterController.State).Should(Equal(clusterController.State))
        })

        Context("A node is restarting after being disconnected from the cluster for some time.", func() {
            Specify("If the state was compacted at other nodes then this node should receive a snapshot from them first", func() {
            })

            Specify("If the state was not yet compacted at other nodes then this node should receive a series of cluster commands  allowing its state to catch up", func() {
            })
        })
    })

    Describe("Adding a node to a cluster", func() {
        Specify("All existing cluster nodes should receive the new node's address before sending it messages", func() {
            // This is important for any external transport module that needs to know where a node resides before communicating
            // with it
        })

        Specify("If a majority of cluster nodes are not available when requesting that a node be added the addition should fail", func() {
        })
    })

    Describe("A node resuming communication with the cluster after being unable to communicate with the majority for some time", func() {
        Context("A snapshot has occurred", func() {
            Specify("The node should catch up with the rest of the cluster by receiving a snapshot from the leader", func() {
            })
        })

        Context("No snapshot has occurred", func() {
            Specify("The node should catch up with the rest of the cluster by receiving a series of cluster commands", func() {
            })
        })
    })

    Describe("Removing a node from cluster", func() {
        Specify("If a majority of cluster nodes are not available when requesting that a node be removed the removal should fail", func() {
        })
    })
    
    Describe("Replacing a node", func() {
        Context("The specified replacement node doesnt belong to the cluster", func() {
        })

        Context("The specified replacement node already has tokens assigned to it", func() {
        })
        
        Context("The specified replacement node exists and doesnt have tokens assigned to it", func() {
        })

        Specify("", func() {
            // Makes node addition proposal for new node with capacity zero so no tokens are assigned to it
            // Once it has been added make node removal proposal with flag to indicate intent to give all its tokens to the new node
            // When committed the node removal should be cancelled if the other node no longer exists in the config (maybe it was removed quickly after)
            // If not cancelled here then give all of the old node's tokens to the new node and apply the configuration change
        })
    })
})

// Node ip change?
// should generate node id based on a name?
