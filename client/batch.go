package client

import (
    "devicedb/transport"
)

type Batch struct {
    ops map[string]transport.TransportUpdateOp
}

func NewBatch() *Batch {
    return &Batch{
        ops: make(map[string]transport.TransportUpdateOp),
    }
}

func (batch *Batch) Put(key string, value string, context string) *Batch {
    batch.ops[key] = transport.TransportUpdateOp{
        Type: "put",
        Key: key,
        Value: value,
        Context: context,
    }

    return batch
}

func (batch *Batch) Delete(key string, context string) *Batch {
    batch.ops[key] = transport.TransportUpdateOp{
        Type: "delete",
        Key: key,
        Context: context,
    }

    return batch
}

func (batch *Batch) ToTransportUpdateBatch() transport.TransportUpdateBatch {
    var updateBatch []transport.TransportUpdateOp = make([]transport.TransportUpdateOp, 0, len(batch.ops))

    for _, op := range batch.ops {
        updateBatch = append(updateBatch, op)
    }

    return transport.TransportUpdateBatch(updateBatch)
}