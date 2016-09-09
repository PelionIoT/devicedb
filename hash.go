package halodb

import (
    "crypto/md5"
    "encoding/binary"
)

const (
    HASH_SIZE_BYTES = 16
)

type Hash struct {
    hash[2] uint64
}

func NewHash(input []byte) Hash {
    var newHash Hash
    
    sum := md5.Sum(input)
    
    newHash.hash[0] = binary.BigEndian.Uint64(sum[0:8])
    newHash.hash[1] = binary.BigEndian.Uint64(sum[8:16])
    
    return newHash
}

func (hash Hash) Xor(otherHash Hash) Hash {
    return Hash{[2]uint64{ hash.hash[0] ^ otherHash.hash[0], hash.hash[1] ^ otherHash.hash[1] }}
}

func (hash Hash) Bytes() [16]byte {
    var result [16]byte
    
    binary.BigEndian.PutUint64(result[0:8], hash.hash[0])
    binary.BigEndian.PutUint64(result[8:16], hash.hash[1])
    
    return result
}

func (hash Hash) Low() uint64 {
    return hash.hash[0]
}

func (hash Hash) SetLow(l uint64) Hash {
    hash.hash[0] = l
    
    return hash
}

func (hash Hash) High() uint64 {
    return hash.hash[1]
}

func (hash Hash) SetHigh(h uint64) Hash {
    hash.hash[1] = h
    
    return hash
}
