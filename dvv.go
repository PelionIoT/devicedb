package halodb

import (
    "encoding/binary"
    "encoding/gob"
    "bytes"
)

type Dot struct {
    nodeID string
    count uint64
}

type DVV struct {
    dot Dot
    vv map[string]uint64
}

func NewDot(nodeID string, count uint64) *Dot {
    return &Dot{nodeID, count}
}

func NewDVV(dot *Dot, vv map[string]uint64) *DVV {
    return &DVV{*dot, vv}
}

func (dvv *DVV) Dot() Dot {
    return dvv.dot
}

func (dvv *DVV) Context() map[string]uint64 {
    return dvv.vv
}

func (dvv *DVV) HappenedBefore(otherDVV *DVV) bool {
    if _, ok := otherDVV.vv[dvv.Dot().nodeID]; ok {
        return dvv.Dot().count <= otherDVV.Context()[dvv.Dot().nodeID]
    }
    
    return false
}

func (dvv *DVV) Replicas() []string {
    replicas := make([]string, 0, len(dvv.Context()) + 1)
    dotNodeID := dvv.Dot().nodeID
    
    for nodeID, _ := range dvv.Context() {
        replicas = append(replicas, nodeID)
        
        if nodeID == dotNodeID {
            dotNodeID = ""
        }
    }
    
    if len(dotNodeID) > 0 {
        replicas = append(replicas, dotNodeID)
    }
    
    return replicas
}

func (dvv *DVV) MaxDot(nodeID string) uint64 {
    var maxDot uint64
    
    if dvv.Dot().nodeID == nodeID {
        if dvv.Dot().count > maxDot {
            maxDot = dvv.Dot().count
        }
    }
    
    if _, ok := dvv.Context()[nodeID]; ok {
        if dvv.Context()[nodeID] > maxDot {
            maxDot = dvv.Context()[nodeID]
        }
    }
    
    return maxDot
}

func (dvv *DVV) Equals(otherDVV *DVV) bool {
    if dvv.Dot().nodeID != otherDVV.Dot().nodeID || dvv.Dot().count != otherDVV.Dot().count {
        return false
    }
    
    if len(dvv.Context()) != len(otherDVV.Context()) {
        return false
    }

    for nodeID, count := range dvv.Context() {
        if _, ok := otherDVV.Context()[nodeID]; !ok {
            return false
        }
        
        if count != otherDVV.Context()[nodeID] {
            return false
        }
    }
    
    return true
}

func (dvv *DVV) Hash() Hash {
    var hash Hash
    
    for nodeID, count := range dvv.Context() {
        countBuffer := make([]byte, 8)
        binary.BigEndian.PutUint64(countBuffer, count)
        
        hash = hash.Xor(NewHash([]byte(nodeID))).Xor(NewHash(countBuffer))
    }
    
    countBuffer := make([]byte, 8)
    binary.BigEndian.PutUint64(countBuffer, dvv.Dot().count)
    
    hash = hash.Xor(NewHash([]byte(dvv.Dot().nodeID))).Xor(NewHash(countBuffer))
    
    return hash
}

func (dvv *DVV) MarshalBinary() ([]byte, error) {
    var encoding bytes.Buffer
    encoder := gob.NewEncoder(&encoding)
    
    encoder.Encode(dvv.Context())
    encoder.Encode(dvv.Dot().nodeID)
    encoder.Encode(dvv.Dot().count)
    
    return encoding.Bytes(), nil
}

func (dvv *DVV) UnmarshalBinary(data []byte) error {
    var dot Dot
    var versionVector map[string]uint64
    
    encoding := bytes.NewBuffer(data)
    decoder := gob.NewDecoder(encoding)
    
    decoder.Decode(&versionVector)
    decoder.Decode(&dot.nodeID)
    decoder.Decode(&dot.count)
    
    dvv.dot = dot
    dvv.vv = versionVector
    
    return nil
}
