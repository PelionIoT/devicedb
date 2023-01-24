package transfer_test

//
// Copyright (c) 2019 ARM Limited.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

import (
	. "github.com/PelionIoT/devicedb/cluster"
	. "github.com/PelionIoT/devicedb/raft"
	. "github.com/PelionIoT/devicedb/transfer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TransferPartnerStrategy", func() {
	Describe("RandomTransferPartnerStrategy", func() {
		Describe("#ChooseTransferPartner", func() {
			Context("When there are no known nodes that currently hold a replica of the specified partition", func() {
				It("should return 0", func() {
					clusterController := &ClusterController{
						LocalNodeID: 1,
						State: ClusterState{
							ClusterSettings: ClusterSettings{
								Partitions:        1024,
								ReplicationFactor: 2,
							},
						},
					}
					clusterController.State.Initialize()

					configController := NewConfigController(nil, nil, clusterController)
					transferStrategy := NewRandomTransferPartnerStrategy(configController)

					Expect(transferStrategy.ChooseTransferPartner(0)).Should(Equal(uint64(0)))
				})
			})

			Context("When there is at least one node that currently holds a replica of the specified partition", func() {
				It("should randomly choose one of ids of those nodes to return", func() {
					clusterController := &ClusterController{
						LocalNodeID: 1,
						State: ClusterState{
							ClusterSettings: ClusterSettings{
								Partitions:        1024,
								ReplicationFactor: 3,
							},
						},
					}
					clusterController.State.Initialize()
					clusterController.State.AddNode(NodeConfig{Address: PeerAddress{NodeID: 1}, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{}})
					clusterController.State.AddNode(NodeConfig{Address: PeerAddress{NodeID: 2}, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{}})
					clusterController.State.AddNode(NodeConfig{Address: PeerAddress{NodeID: 3}, Capacity: 1, PartitionReplicas: map[uint64]map[uint64]bool{}})
					clusterController.State.AssignPartitionReplica(0, 0, 1)
					clusterController.State.AssignPartitionReplica(0, 1, 2)
					clusterController.State.AssignPartitionReplica(0, 2, 3)

					configController := NewConfigController(nil, nil, clusterController)
					transferStrategy := NewRandomTransferPartnerStrategy(configController)

					occurrences := make(map[uint64]bool, 0)

					for i := 0; i < 1000; i += 1 {
						partner := transferStrategy.ChooseTransferPartner(0)

						occurrences[partner] = true
					}

					Expect(len(occurrences)).Should(Equal(3))
				})
			})
		})
	})
})
