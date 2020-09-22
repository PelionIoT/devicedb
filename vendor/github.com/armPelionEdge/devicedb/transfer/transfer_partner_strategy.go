package transfer
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
    "math/rand"

    . "github.com/armPelionEdge/devicedb/cluster"
)

type PartitionTransferPartnerStrategy interface {
    ChooseTransferPartner(partition uint64) uint64
}

type RandomTransferPartnerStrategy struct {
    configController ClusterConfigController
}

func NewRandomTransferPartnerStrategy(configController ClusterConfigController) *RandomTransferPartnerStrategy {
    return &RandomTransferPartnerStrategy{
        configController: configController,
    }
}

func (partnerStrategy *RandomTransferPartnerStrategy) ChooseTransferPartner(partition uint64) uint64 {
    holders := partnerStrategy.configController.ClusterController().PartitionHolders(partition)

    if len(holders) == 0 {
        return 0
    }

    // randomly choose a holder to transfer from
    return holders[rand.Int() % len(holders)]
}

// Choose a partner
// The node that needs to perform a partition transfer prioritizes transfer
// partners like so from best candidate to worst:
//   1) A node that is a holder of a replica that this node now owns
//   2) A node that is a holder of some replica of this partition but not one that overlaps with us
