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
    "io"
    "encoding/json"
)

type PartitionTransferEncoder interface {
    Encode() (io.Reader, error)
}

type PartitionTransferDecoder interface {
    Decode() (PartitionTransfer, error)
}

type TransferEncoder struct {
    transfer PartitionTransfer
    reader io.Reader
}

func NewTransferEncoder(transfer PartitionTransfer) *TransferEncoder {
    return &TransferEncoder{
        transfer: transfer,
    }
}

func (encoder *TransferEncoder) Encode() (io.Reader, error) {
    if encoder.reader != nil {
        return encoder.reader, nil
    }

    encoder.reader = &JSONPartitionReader{
        PartitionTransfer: encoder.transfer,
    }

    return encoder.reader, nil
}

type JSONPartitionReader struct {
    PartitionTransfer PartitionTransfer
    needsDelimiter bool
    currentChunk []byte
}

func (partitionReader *JSONPartitionReader) Read(p []byte) (n int, err error) {
    for len(p) > 0 {
        if len(partitionReader.currentChunk) == 0 {
            chunk, err := partitionReader.nextChunk()

            if err != nil {
                return n, err
            }

            if chunk == nil {
                return n, io.EOF
            }

            partitionReader.currentChunk = chunk
        }

        if partitionReader.needsDelimiter {
            nCopied := copy(p, []byte("\n"))
            p = p[nCopied:]
            n += nCopied
            partitionReader.needsDelimiter = false
        }

        nCopied := copy(p, partitionReader.currentChunk)
        p = p[nCopied:]
        n += nCopied
        partitionReader.currentChunk = partitionReader.currentChunk[nCopied:]

        // if we have copied a whole chunk the next character needs to be a delimeter
        partitionReader.needsDelimiter = len(partitionReader.currentChunk) == 0
    }

    return n, nil
}

func (partitionReader *JSONPartitionReader) nextChunk() ([]byte, error) {
    nextChunk, err := partitionReader.PartitionTransfer.NextChunk()

    if err != nil {
        return nil, err
    }

    if nextChunk.IsEmpty() {
        return nil, nil
    }

    return json.Marshal(nextChunk)
}

type TransferDecoder struct {
    transfer PartitionTransfer
}

func NewTransferDecoder(reader io.Reader) *TransferDecoder {
    return &TransferDecoder{
        transfer: NewIncomingTransfer(reader),
    }
}

func (decoder *TransferDecoder) Decode() (PartitionTransfer, error) {
    return decoder.transfer, nil
}