package util

import (
    "encoding/binary"
    "crypto/rand"
)

func UUID64() uint64 {
    randomBytes := make([]byte, 8)
    rand.Read(randomBytes)
    
    return binary.BigEndian.Uint64(randomBytes[:8])
}