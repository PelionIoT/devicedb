package clusterio

import (
    "context"
    "sync"
    "time"

    . "devicedb/bucket"
    . "devicedb/data"
    . "devicedb/error"
    . "devicedb/logging"
)

type getResult struct {
    nodeID uint64
    siblingSets []*SiblingSet
}

type getMatchesResult struct {
    nodeID uint64
    siblingSetIterator SiblingSetIterator
}

type Agent struct {
    PartitionResolver PartitionResolver
    NodeClient NodeClient
    NodeReadRepairer NodeReadRepairer
    Timeout time.Duration
    mu sync.Mutex
    nextOperationID uint64
    operationCancellers map[uint64]func()
}

func NewAgent() *Agent {
    return &Agent{
        operationCancellers: make(map[uint64]func(), 0),
    }
}

func (agent *Agent) Batch(ctx context.Context, siteID string, bucket string, updateBatch *UpdateBatch) (int, int, error) {
    var partitionNumber uint64 = agent.PartitionResolver.Partition(siteID)
    var replicaNodes []uint64 = agent.PartitionResolver.ReplicaNodes(partitionNumber)
    var nApplied int = 0
    var nFailed int = 0
    var applied chan int = make(chan int, len(replicaNodes))
    var failed chan int = make(chan int, len(replicaNodes))

    opID, ctxDeadline := agent.newOperation(ctx)

    for _, nodeID := range replicaNodes {
        go func(nodeID uint64) {
            if err := agent.NodeClient.Batch(ctxDeadline, nodeID, partitionNumber, siteID, bucket, updateBatch); err != nil {
                Log.Errorf("Unable to replicate batch update to bucket %s at site %s at node %d: %v", bucket, siteID, nodeID, err.Error())

                failed <- 1

                return
            }

            applied <- 1
        }(nodeID)
    }

    var quorumReached chan int = make(chan int)
    var allAttemptsMade chan int = make(chan int, 1)

    go func() {
        // All calls to NodeClient.Batch() will succeed, receive a failure reponse from the specified peer, or time out eventually
        for nApplied + nFailed < len(replicaNodes) {
            select {
            case <-failed:
                // indicates that a call to NodeClient.Batch() either received an error response from the specified node or timed out
                nFailed++
            case <-applied:
                // indicates that a call to NodeClient.Batch() received a success response from the specified node
                nApplied++

                if nApplied == agent.NQuorum(len(replicaNodes)) {
                    // if a quorum of nodes have successfully been written to then the Batch() function should return successfully
                    // without waiting for the rest of the responses to come in. However, this function should continue to run
                    // until the deadline is reached or a response (whether it be failure or success) is received for each call
                    // to Batch() to allow this update to propogate to all replicas
                    quorumReached <- nApplied
                }
            }
        }

        // Once the deadline is reached or all calls to Batch() have received a response this channel should be written
        // to so that if Batch() is still waiting to return (since quorum was never reached) it will return an error response
        // indicating that quorum was not establish and the update was not successfully applied
        allAttemptsMade <- nApplied
        // Do this to remove the canceller from the map even though the operation is already done
        agent.cancelOperation(opID)
    }()

    select {
    case n := <-allAttemptsMade:
        return len(replicaNodes), n, ENoQuorum
    case n := <-quorumReached:
        return len(replicaNodes), n, nil
    }
}

func (agent *Agent) Get(ctx context.Context, siteID string, bucket string, keys [][]byte) ([]*SiblingSet, error) {
    var partitionNumber uint64 = agent.PartitionResolver.Partition(siteID)
    var replicaNodes []uint64 = agent.PartitionResolver.ReplicaNodes(partitionNumber)
    var readMerger *ReadMerger = NewReadMerger()
    var readResults chan getResult = make(chan getResult, len(replicaNodes))
    var failed chan int = make(chan int, len(replicaNodes))
    var nRead int = 0
    var nFailed int = 0

    opID, ctxDeadline := agent.newOperation(ctx)

    for _, nodeID := range replicaNodes {
        go func(nodeID uint64) {
            siblingSets, err := agent.NodeClient.Get(ctxDeadline, nodeID, partitionNumber, siteID, bucket, keys)

            if err != nil {
                Log.Errorf("Unable to get keys from bucket %s at site %s at node %d: %v", bucket, siteID, nodeID, err.Error())

                failed <- 1

                return
            }

            readResults <- getResult{ nodeID: nodeID, siblingSets: siblingSets }
        }(nodeID)
    }

    var mergedResult chan []*SiblingSet = make(chan []*SiblingSet)
    var allAttemptsMade chan int = make(chan int, 1)

    go func() {
        for nRead + nFailed < len(replicaNodes) {
            select {
            case <-failed:
                nFailed++
            case r := <-readResults:
                nRead++

                for i, key := range keys {
                    readMerger.InsertKeyReplica(r.nodeID, string(key), r.siblingSets[i])
                }

                if nRead == agent.NQuorum(len(replicaNodes)) {
                    // calculate result set
                    var resultSet []*SiblingSet = make([]*SiblingSet, len(replicaNodes))

                    for i, key := range keys {
                        resultSet[i] = readMerger.Get(string(key))
                    }

                    mergedResult <- resultSet
                }
            }
        }

        agent.NodeReadRepairer.BeginRepair(readMerger)
        allAttemptsMade <- 1
        agent.cancelOperation(opID)
    }()

    select {
    case result := <-mergedResult:
        return result, nil
    case <-allAttemptsMade:
        return nil, ENoQuorum
    }
}

func (agent *Agent) GetMatches(ctx context.Context, siteID string, bucket string, keys [][]byte) (SiblingSetIterator, error) {
    var partitionNumber uint64 = agent.PartitionResolver.Partition(siteID)
    var replicaNodes []uint64 = agent.PartitionResolver.ReplicaNodes(partitionNumber)
    var readMerger *ReadMerger = NewReadMerger()
    var mergeIterator *SiblingSetMergeIterator = NewSiblingSetMergeIterator(readMerger)
    var readResults chan getMatchesResult = make(chan getMatchesResult, len(replicaNodes))
    var failed chan int = make(chan int, len(replicaNodes))
    var nRead int = 0
    var nFailed int = 0

    opID, ctxDeadline := agent.newOperation(ctx)

    for _, nodeID := range replicaNodes {
        go func(nodeID uint64) {
            ssIterator, err := agent.NodeClient.GetMatches(ctxDeadline, nodeID, partitionNumber, siteID, bucket, keys)

            if err != nil {
                Log.Errorf("Unable to get matches from bucket %s at site %s at node %d: %v", bucket, siteID, nodeID, err.Error())

                failed <- 1

                return
            }

            readResults <- getMatchesResult{ nodeID: nodeID, siblingSetIterator: ssIterator }
        }(nodeID)
    }

    var quorumReached chan int = make(chan int)
    var allAttemptsMade chan int = make(chan int, 1)

    go func() {
        for nFailed + nRead < len(replicaNodes) {
            select {
            case <-failed:
                nFailed++
            case result := <-readResults:
                for result.siblingSetIterator.Next() {
                    readMerger.InsertKeyReplica(result.nodeID, string(result.siblingSetIterator.Key()), result.siblingSetIterator.Value())
                    mergeIterator.AddKey(string(result.siblingSetIterator.Prefix()), string(result.siblingSetIterator.Key()))
                }

                if result.siblingSetIterator.Error() != nil {
                    nFailed++
                } else {
                    nRead++
                }

                if nRead == agent.NQuorum(len(replicaNodes)) {
                    quorumReached <- 1
                }
            }
        }

        agent.NodeReadRepairer.BeginRepair(readMerger)
        allAttemptsMade <- 1
        agent.cancelOperation(opID)
    }()

    select {
    case <-allAttemptsMade:
        return nil, ENoQuorum
    case <-quorumReached:
        mergeIterator.SortKeys()
        return mergeIterator, nil
    }
}

func (agent *Agent) newOperation(ctx context.Context) (uint64, context.Context) {
    agent.mu.Lock()
    defer agent.mu.Unlock()

    var id uint64 = agent.nextOperationID
    agent.nextOperationID++

    ctxDeadline, cancel := context.WithTimeout(ctx, agent.Timeout)

    agent.operationCancellers[id] = cancel

    return id, ctxDeadline
}

func (agent *Agent) cancelOperation(id uint64) {
    agent.mu.Lock()
    defer agent.mu.Unlock()

    if cancel, ok := agent.operationCancellers[id]; ok {
        cancel()
        delete(agent.operationCancellers, id)
    }
}

func (agent *Agent) NQuorum(replicas int) int {
    return (replicas / 2) + 1
}

func (agent *Agent) CancelAll() {
    agent.mu.Lock()
    defer agent.mu.Unlock()

    agent.NodeReadRepairer.StopRepairs()
    
    for id, cancel := range agent.operationCancellers {
        cancel()
        delete(agent.operationCancellers, id)
    }
}