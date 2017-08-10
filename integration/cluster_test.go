package integration_test

import (
    . "github.com/onsi/ginkgo"
    . "github.com/onsi/gomega"
)

var _ = Describe("Cluster Operation", func() {
    Describe("Failure Modes And Recovery", func() {
        Context("When a node is dead and needs replacement", func() {
            Context("The node is a primary replica owner but not holder", func() {
                Context("RF > 1", func() {
                    Context("The primary replica holder is still around", func() {
                        Specify("The replacement node should start normal replica transfer procedures to get the replica data", func() {
                            // This just resembles a normal transfer procedure
                            Fail("Not implemented")
                        })
                    })

                    Context("The primary replica holder was removed and all backup nodes were removed", func() {
                        Specify("The replacement node should propose a holder transfer then start pushing backups to whichever nodes are the new backup nodes", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("The primary replica holder was removed but at least one backup node exists", func() {
                        Specify("The replacement node should query each backup node to see which is most up to date and restore its state from that one", func() {
                            Fail("Not implemented")
                        })

                        Specify("If a certain backup node does not contain the backup replica the replacement node thinks it does keep trying until it agrees", func() {
                            Fail("Not implemented")
                        })

                        Specify("If a certain backup node doesn not believe the replacement node to be the primary replica holder it should wait", func() {
                            Fail("Not implemented")
                        })

                        Specify("Once the replacement node has queried all backup nodes to see which one is most up to date it should restore its state from the most up to date backup", func() {
                            Fail("Not implemented")
                        })

                        Specify("Once a node has successfully restored its state from a backup node it should propose a holder transfer", func() {
                            Fail("Not implemented")
                        })

                        Specify("Once the holder transfer is committed and confirmed valid the node should start forwarding updates to backup nodes", func() {
                            Fail("Not implemented")
                        })

                        Specify("Once the holder transfer is committed and confirmed valid the node should start accepting writes for that partition", func() {
                            Fail("Not implemented")
                        })
                    })
                })

                Context("RF = 1", func() {
                    Context("The primary replica holder is still around", func() {
                        Specify("The replacement node should start normal replica transfer procedures to get the replica data", func() {
                            Fail("Not implemented")
                        })
                    })

                    Context("The primary replica holder was removed", func() {
                        Specify("The replacement node should propose a holder transfer then accepting new updates", func() {
                            Fail("Not implemented")
                        })
                    })
                })
            })

            Context("The node is a primary replica holder but not owner", func() {
                Specify("The replacement node should not be a holder for this replica", func() {
                    // In other words the partition holder flag should be cleared once this node replaces another
                    // for the primary replica of this partition. This lets the new owner know that it will not be able to obtain
                    // a transfer from anyone since that data no longer exists.
                    Fail("Not implemented")
                })
            })

            Context("The node is a primary replica owner and holder", func() {
                Specify("The replacement node should not be a holder for this replica", func() {
                    Fail("Not implemented")
                })

                // This is the exact same as the first case after the holder status is reset. This node 
                // will need to obtain data then transfer holder
            })

            Context("The node is a backup replica owner but not holder", func() {
                Context("The backup partition still has a holder", func() {
                    Specify("The replacement node should attempt to transfer the backup from this node", func() {
                        Fail("Not implemented")
                    })
                })

                Context("The backup partition does not have a holder", func() {
                    Specify("The replacement node should propose a holder transfer", func() {
                        Fail("Not implemented")
                    })

                    Specify("Upon successfully committing a holder transfer the backup replica holder should begin accepting pushes from the primary replica", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("The node is a backup replica holder but not owner", func() {
                Specify("The replacement node should not be a holder for this replica", func() {
                    Fail("Not implemented")
                })
            })

            Context("The node is a backup replica owner and holder", func() {
                Specify("The replacement node should not be a holder for this replica", func() {
                    Fail("Not implemented")
                })
            })
        })

        Context("All nodes holding the replicas for a partition have been forcefully removed since they were dead", func() {
            Specify("The new owner for the primary replica should start accepting writes after committing a holder transfer", func() {
                Fail("Not Implemented")
                // What if this node is behind an catching up in the commit log and starts accepting writes?
                // This may result in two nodes at the same time believing themselves to be primary replica
                // owners for a partition and both see a view where there are no partition replicas so both
                // are accepting writes for the same partition
                // Simultaneous views...
                // N1: [N1 Add] [N1 Gain Token 1] [N2 Add] [N2 Gain Token 1] ... [N2 Remove] [N1 Gain Token 1]
                // N2: [N1 Add] [N1 Gain Token 1] [N2 Add] [N2 Gain Token 1]
                // N3: [N1 Add] [N1 Gain Token 1] [N2 Add] [N2 Gain Token 1] ... [N2 Remove] [N1 Gain Token 1]
                // N4: [N1 Add] [N1 Gain Token 1] [N2 Add] [N2 Gain Token 1] ... [N2 Remove]
                // N5: [N1 Add] [N1 Gain Token 1] [N2 Add] [N2 Gain Token 1] ... [N2 Remove]
                // N1 believes it owns token 1
                // N2 believes it owns token 1
                // This would only be a problem if the node is still running when it is forcefully removed
                // If it was gracefully decommissioned then N1 would gain token 1 at the end leading to it owning the primary partition
                // but it would then wait for the transfer from its current holder, N2.
                // Advise operators to only use remove if ensured that it is dead
                // Can this problem ever arise with graceful decommissions?
                // Assuming only graceful removals:
                //   Initiating a partition transfer implies that both the tranferrer and the receiver agree that the receiver is the owner of the partition
                //   Which implies that the transferrer has write-locked the partition locally
                //   Successfully finalizing a partition transfer implies that both the has successfully committed a holder change for that partition
                //     and is still the owner of that partition at the time the transfer is committed
                //   The old holder may not write to that partition again once it loses ownership. If it gains ownership again it 
                // What if the majority of the cluster agrees node 3 is the owner of partition 1 but nodes 4 and 5 are behind and both think that 5 owns
                // the partition and that node 4 holds it? 5 would be able to initiate a transfer but be unable to commit the hold before learning that its
                // not the real owner, in which case node 3 would start its transfer from node 4
                //
                // Rule added to state machine: partition transfer only does something if the owner == the new holder
                // The invariant that ensures safety:
                // In order for a node to accept writes to a partition it must be both the holder and the owner of that partition
                // In order to become the holder of a partition the current holder
                // Partitions pass directly from one holder to the next. This means that for a node to have a writable partition
                // it must have spoke with the last holder about the transfer, the last holder agreed to it, and the transfer was 
                // committed to the log in an order such that nobody took ownership of the partition before the transfer was complete
            })
        })
    })

    // TODO change this. A backup transfers from the old backup partition holder, not from primary
    // Ex: If a node owns partition 1 replica 1 now it should transfer from the holder of replica 1, not obtain updates
    // from the primary. This is essential for primary restoration to work as described above

    // Primary Partition 1 Replica:
    //   Backup Replica 1: Has been sent up to update 45
    //   Backup Replica 2: Has been sent up to update 88
    Describe("Propogating Partition State To Backups", func() {
        Specify("A primary partition replica node should keep a running count for each backup replica for that partition indicating the last known update to be forwarded to that replica", func() {
            Fail("Not implemented")
        })

        Specify("A primary partition replica should reset the last received index to 0 for a backup replica when the backup replica is assigned to a new node", func() {
            Fail("Not implemented")
        })

        // This context corresponds with the case where the count is reset to 0
        // This concerns snapshots
        Context("The last received index for a backup node as seen by the primary node is less than the earliest update contained in the primary partition replicas log", func() {
            Specify("The node should attempt to send a snapshot of the current partition state to that node", func() {
                // Gets the backup node caught up
                Fail("Not implemented")
            })

            Context("The backup node believes itself to be the owner of that backup replica", func() {
                Context("The backup node has received all updates for that partition up to or past the earliest update stored in the primary's log", func() {
                    It("should respond to the snapshot transfer indicating that it does not need a full transfer and hinting at its latest index", func() {
                        // For example when partition 1 replica 1 is now assigned to a node that previously stored partition 1 replica 2. That node would have data that was fairly up to date
                        // regarding that partition
                        Fail("Not implemented")
                    })

                    Specify("The primary partition should follow up this response by skipping ahead the last received index for that replica and resuming normal forwarding process", func() {
                        Fail("Not implemented")
                    })
                })
            })

            Context("The backup node does not believe itself to be the owner of that backup replica", func() {
                Specify("It should reject the transfer request", func() {
                    Fail("Not implemented")
                })
            })

            Context("The backup node loses ownership of the replica while the snapshot is being transferred", func() {
                Specify("It should cancel the transfer request", func() {
                    Fail("Not implemented")
                })
            })

            Specify("The snapshot transfer should be cancelled if the backup node loses ownership of a backup replica for this partition", func() {
                Fail("Not implemented")
            })
        })

        // Ensure that log entries only get purged once they have been forwarded to all backups
        Specify("A primary node should not purge updates from its log until the last received index for all backup replicas is >= that update's index", func() {
            Fail("Not implemented")
        })

        // Normal forwarding process
        Specify("A primary partition replica node should continuously attempt to forward updates to all backup nodes until they are caught up", func() {
            Fail("Not implemented")
        })

        Specify("A primary partition replica should skip ahead its last received index for a backup replica if the backup replica already contains some updates", func() {
            Fail("Not implemented")
        })

        Specify("A primary partition replica node should cancel any outgoing snapshot transfers if it loses ownership over that partition", func() {
            Fail("Not implemented")
        })

        Specify("A primary partition replica node should cease any outgoing update pushes if it loses ownership over that partition", func() {
            Fail("Not Implemented")
            Expect(true).Should(BeTrue())
        })
    })

    Describe("Resizing and Rebalancing", func() {
        Context("There are no backup nodes for this partition (replication factor = 1)", func() {
        })

        Context("There is at least one backup node for this partition (replication factor > 1 and #nodes > 1)", func() {
            Context("All backup nodes are available", func() {
            })

            Context("Not all backup nodes are available", func() {
                Specify("The data transfer from the backup nodes to the new owner of the primary partition replica should block until the other backup node becomes available again", func() {
                    Fail("Not Implemented")
                })
            })
        })
    })
})
