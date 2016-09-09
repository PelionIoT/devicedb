package devicedb

import (
    "encoding/gob"
    "bytes"
)

type Sibling struct {
    clock *DVV
    value []byte
    timestamp uint64
}

func NewSibling(clock *DVV, value []byte, timestamp uint64) *Sibling {
    return &Sibling{clock, value, timestamp}
}

func (sibling *Sibling) Clock() *DVV {
    return sibling.clock
}

func (sibling *Sibling) Value() []byte {
    return sibling.value
}

func (sibling *Sibling) IsTombstone() bool {
    return sibling.value == nil
}

func (sibling *Sibling) Timestamp() uint64 {
    return sibling.timestamp
}

func (sibling *Sibling) Hash() Hash {
    if sibling == nil || sibling.IsTombstone() {
        return Hash{[2]uint64{ 0, 0 }}
    }
    
    return NewHash(sibling.value).Xor(sibling.clock.Hash())
}

func (sibling *Sibling) MarshalBinary() ([]byte, error) {
    var encoding bytes.Buffer
    encoder := gob.NewEncoder(&encoding)
    
    encoder.Encode(sibling.Clock())
    encoder.Encode(sibling.Timestamp())
    encoder.Encode(sibling.Value())
    
    return encoding.Bytes(), nil
}

func (sibling *Sibling) UnmarshalBinary(data []byte) error {
    var clock DVV
    var timestamp uint64
    var value []byte
    
    encoding := bytes.NewBuffer(data)
    decoder := gob.NewDecoder(encoding)
    
    decoder.Decode(&clock)
    decoder.Decode(&timestamp)
    decoder.Decode(&value)
    
    sibling.clock = &clock
    sibling.timestamp = timestamp
    sibling.value = value
    
    return nil
}
