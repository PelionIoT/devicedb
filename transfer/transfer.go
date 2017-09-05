package transfer

import (
    "bufio"
    "io"
    "errors"

    . "devicedb/data"
    . "devicedb/partition"
)

var ECancelled = errors.New("Cancelled")

type PartitionTransfer interface {
    NextChunk() (PartitionChunk, error)
    Cancel()
}

type IncomingTransfer struct {
    scanner *bufio.Scanner
    err error
}

func NewIncomingTransfer(reader io.Reader) *IncomingTransfer {
    return &IncomingTransfer{
        scanner: bufio.NewScanner(reader),
    }
}

func (transfer *IncomingTransfer) NextChunk() (PartitionChunk, error) {
    if transfer.err != nil {
        return nil, transfer.err
    }

    if !transfer.scanner.Scan() {
        if transfer.scanner.Err() != nil {
            return nil, transfer.scanner.Err()
        }

        return nil, nil
    }

    encoded := transfer.scanner.Bytes()
    var nextPartitionChunk PartitionChunk

    if err := json.Unmarshal(encoded, &nextPartitionChunk); err != nil {
        transfer.err = err

        return nil, transfer.err
    }

    return nextPartitionChunk, nil
}

func (transfer *IncomingTransfer) Cancel() {
}

type OutgoingTransfer struct {
    partitionIterator PartitionIterator
    chunkSize int
    nextChunkIndex uint64
    err error
}

func NewOutgoingTransfer(partition PartitionReplica) *OutgoingTransfer {
    return &OutgoingTransfer{
        partitionIterator: partition.Iterator(),
        chunkSize: 100,
    }
}

func (transfer *OutgoingTransfer) NextChunk() (PartitionChunk, error) {
    if transfer.err != nil {
        return nil, transfer.err
    }

    entries := make([]Entry, 0, transfer.chunkSize)

    for transfer.partitionIterator.Next() {
        entry := Entry{
            Site: transfer.partitionIterator.Site(),
            Bucket: transfer.partitionIterator.Bucket(),
            Key: transfer.partitionIterator.Key(),
            Value: transfer.partitionIterator.Value(),
        }

        entries = append(entries, entry)

        if len(entries) == transfer.chunkSize {
            return &PartitionChunk{
                Index: transfer.nextChunkIndex++,
                Entries: entries,
                Checksum: checksum,
            }
        }
    }

    if transfer.partitionIterator.Error() != nil {
        transfer.err = transfer.partitionIterator.Error()

        return nil, transfer.err
    }

    transfer.err = io.EOF

    return nil, transfer.err
}

func (transfer *OutgoingTransfer) Cancel() {
    if transfer.err != nil {
        transfer.err = ECancelled
    }

    return transfer.partitionIterator.Release()
}
