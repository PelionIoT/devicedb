package transfer_test

import (
    . "devicedb/transfer"
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