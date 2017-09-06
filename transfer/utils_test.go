package transfer_test

import (
    "fmt"
    "net"
    "net/http"

    . "devicedb/transfer"
    . "devicedb/site"
    . "devicedb/partition"
    . "devicedb/data"
)

type mockNextChunkResponse struct {
    chunk PartitionChunk
    err error
}

type MockPartitionTransfer struct {
    nextChunkCalls int
    cancelCalls int
    expectedResponses []mockNextChunkResponse
}

func NewMockPartitionTransfer() *MockPartitionTransfer {
    return &MockPartitionTransfer{
        nextChunkCalls: 0,
        cancelCalls: 0,
        expectedResponses: make([]mockNextChunkResponse, 0),
    }
}

func (mockPartitionTransfer *MockPartitionTransfer) AppendNextChunkResponse(chunk PartitionChunk, err error) *MockPartitionTransfer {
    mockPartitionTransfer.expectedResponses = append(mockPartitionTransfer.expectedResponses, mockNextChunkResponse{ chunk: chunk, err: err })

    return mockPartitionTransfer
}

func (mockPartitionTransfer *MockPartitionTransfer) NextChunkCallCount() int {
    return mockPartitionTransfer.nextChunkCalls
}

func (mockPartitionTransfer *MockPartitionTransfer) CancelCallCount() int {
    return mockPartitionTransfer.cancelCalls
}

func (mockPartitionTransfer *MockPartitionTransfer) NextChunk() (PartitionChunk, error) {
    mockPartitionTransfer.nextChunkCalls++

    if len(mockPartitionTransfer.expectedResponses) == 0 {
        return PartitionChunk{}, nil
    }

    nextResponse := mockPartitionTransfer.expectedResponses[0]
    mockPartitionTransfer.expectedResponses = mockPartitionTransfer.expectedResponses[1:]

    return nextResponse.chunk, nextResponse.err
}

func (mockPartitionTransfer *MockPartitionTransfer) Cancel() {
    mockPartitionTransfer.cancelCalls++
}

type MockPartition struct {
    partition uint64
    replica uint64
    iterator *MockPartitionIterator
}

func NewMockPartition(partition uint64, replica uint64) *MockPartition {
    return &MockPartition{
        partition: partition,
        replica: replica,
        iterator: NewMockPartitionIterator(),
    }
}

func (partition *MockPartition) Partition() uint64 {
    return partition.partition
}

func (partition *MockPartition) Replica() uint64 {
    return partition.replica
}

func (partition *MockPartition) Sites() SitePool {
    return nil
}

func (partition *MockPartition) Iterator() PartitionIterator {
    return partition.iterator
}

func (partition *MockPartition) MockIterator() *MockPartitionIterator {
    return partition.iterator
}

func (partition *MockPartition) LockWrites() {
}

func (partition *MockPartition) UnlockWrites() {
}

func (partition *MockPartition) LockReads() {
}

func (partition *MockPartition) UnlockReads() {
}

type mockIteratorState struct {
    next bool
    site string
    bucket string
    key string
    value *SiblingSet
    checksum Hash
    err error
}

type MockPartitionIterator struct {
    nextCalls int
    siteCalls int
    bucketCalls int
    keyCalls int
    valueCalls int
    checksumCalls int
    releaseCalls int
    errorCalls int
    currentState int
    states []mockIteratorState
}

func NewMockPartitionIterator() *MockPartitionIterator {
    return &MockPartitionIterator{
        currentState: -1,
        states: make([]mockIteratorState, 0),
    }
}

func (partitionIterator *MockPartitionIterator) AppendNextState(next bool, site string, bucket string, key string, value *SiblingSet, checksum Hash, err error) *MockPartitionIterator {
    partitionIterator.states = append(partitionIterator.states, mockIteratorState{
        next: next,
        site: site,
        bucket: bucket,
        key: key,
        value: value,
        checksum: checksum,
        err: err,
    })

    return partitionIterator
}

func (partitionIterator *MockPartitionIterator) Next() bool {
    partitionIterator.nextCalls++
    partitionIterator.currentState++

    return partitionIterator.states[partitionIterator.currentState].next
}

func (partitionIterator *MockPartitionIterator) NextCallCount() int {
    return partitionIterator.nextCalls
}

func (partitionIterator *MockPartitionIterator) Site() string {
    partitionIterator.siteCalls++

    return partitionIterator.states[partitionIterator.currentState].site
}

func (partitionIterator *MockPartitionIterator) SiteCallCount() int {
    return partitionIterator.siteCalls
}

func (partitionIterator *MockPartitionIterator) Bucket() string {
    partitionIterator.bucketCalls++

    return partitionIterator.states[partitionIterator.currentState].bucket
}

func (partitionIterator *MockPartitionIterator) BucketCallCount() int {
    return partitionIterator.bucketCalls
}

func (partitionIterator *MockPartitionIterator) Key() string {
    partitionIterator.keyCalls++

    return partitionIterator.states[partitionIterator.currentState].key
}

func (partitionIterator *MockPartitionIterator) KeyCallCount() int {
    return partitionIterator.keyCalls
}

func (partitionIterator *MockPartitionIterator) Value() *SiblingSet {
    partitionIterator.valueCalls++

    return partitionIterator.states[partitionIterator.currentState].value
}

func (partitionIterator *MockPartitionIterator) ValueCallCount() int {
    return partitionIterator.valueCalls
}

func (partitionIterator *MockPartitionIterator) Checksum() Hash {
    partitionIterator.checksumCalls++

    return partitionIterator.states[partitionIterator.currentState].checksum
}

func (partitionIterator *MockPartitionIterator) ChecksumCallCount() int {
    return partitionIterator.checksumCalls
}

func (partitionIterator *MockPartitionIterator) Release() {
    partitionIterator.releaseCalls++
}

func (partitionIterator *MockPartitionIterator) ReleaseCallCount() int {
    return partitionIterator.releaseCalls
}

func (partitionIterator *MockPartitionIterator) Error() error {
    partitionIterator.errorCalls++

    return partitionIterator.states[partitionIterator.currentState].err
}

func (partitionIterator *MockPartitionIterator) ErrorCallCount() int {
    return partitionIterator.errorCalls
}

type MockErrorReader struct {
    errors []error
}

func NewMockErrorReader(errors []error) *MockErrorReader {
    return &MockErrorReader{
        errors: errors,
    }
}

func (errorReader *MockErrorReader) Read(p []byte) (n int, err error) {
    nextError := errorReader.errors[0]
    errorReader.errors = errorReader.errors[1:]

    return 0, nextError
}

type HTTPTestServer struct {
    server *http.Server
    listener net.Listener
    port int
    done chan int
}

func NewHTTPTestServer(port int, handler http.Handler) *HTTPTestServer {
    return &HTTPTestServer{
        port: port,
        server: &http.Server{
            Addr: fmt.Sprintf(":%d", port),
            Handler: handler,
        },
        done: make(chan int),
    }
}

func (testServer *HTTPTestServer) Start() {
    go func() {
        listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", testServer.port))
        testServer.listener = listener
        testServer.server.Serve(listener)
        close(testServer.done)
    }()
}

func (testServer *HTTPTestServer) Stop() {
    testServer.listener.Close()
    <-testServer.done
}