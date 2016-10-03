// depth   |   memory overhead
// 5       |   512      bytes  (0.5   KiB)
// 10      |   16384    bytes  (16    KiB)
// 15      |   524288   bytes  (512   KiB)
// 19      |   8388608  bytes  (8192  KiB) (8  MiB)
// 20      |   16777216 bytes  (16384 KiB) (16 MiB)

package sync

import (
    "math"
    "errors"
    "unsafe"
    "strconv"
    "devicedb/dbobject"
    "sync"
)

// MerkleMaxDepth should never exceed 32
const MerkleMinDepth uint8 = 1
const MerkleDefaultDepth uint8 = 19
const MerkleMaxDepth uint8 = 28 // 4GB

type MerkleTree struct {
    depth uint8
    nodes []dbobject.Hash
    updateLock sync.Mutex
}

func NewMerkleTree(depth uint8) (*MerkleTree, error) {
    if depth < MerkleMinDepth || depth > MerkleMaxDepth {
        return nil, errors.New("depth must be between " + strconv.Itoa(int(MerkleMinDepth)) + " and " + strconv.Itoa(int(MerkleMaxDepth)))
    }
    
    nodes := make([]dbobject.Hash, uint32(math.Pow(float64(2), float64(depth))))
    
    var updateLock sync.Mutex
    return &MerkleTree{depth, nodes, updateLock}, nil
}

func (tree *MerkleTree) RootHash() dbobject.Hash {
    return tree.nodes[1 << (tree.depth - 1)]
}

func (tree *MerkleTree) RangeHash(rangeMin uint32, rangeMax uint32) dbobject.Hash {
    return tree.nodes[rangeMin + (rangeMax - rangeMin)/2]
}

func (tree *MerkleTree) NodeHash(node uint32) dbobject.Hash {
    if node >= uint32(len(tree.nodes)) {
        return dbobject.Hash{}
    }
    
    return tree.nodes[node]
}

func (tree *MerkleTree) Depth() uint8 {
    return tree.depth
}

func (tree *MerkleTree) SetNodeHashes(nodeHashes map[uint32]dbobject.Hash) {
    for nodeID, hash := range nodeHashes {
        tree.nodes[nodeID] = hash
    }
}

func (tree *MerkleTree) SetNodeHash(nodeID uint32, hash dbobject.Hash) {
    tree.nodes[nodeID] = hash
}

func (tree *MerkleTree) LeafNode(key []byte) uint32 {
    keyHash := dbobject.NewHash(key)
    
    return LeafNode(&keyHash, tree.depth)
}

// this
func (tree *MerkleTree) UpdateLeafHash(nodeID uint32, hash dbobject.Hash) {
    tree.updateLock.Lock()
    
    defer tree.updateLock.Unlock()
    
    if !tree.IsLeaf(nodeID) {
        return
    }
    
    tree.SetNodeHash(nodeID, hash)
    node := nodeID
    
    for node != (1 << (tree.Depth() - 1)) {
        shift := CountTrailingZeros(node) - 1
        left := node - (1 << shift)
        right := node + (1 << shift)
        
        if node & 0x1 != 1 {
            tree.nodes[node] = tree.nodes[left].Xor(tree.nodes[right])
        }
        
        node = ParentNode(node)
    }
}

func (tree *MerkleTree) IsLeaf(nodeID uint32) bool {
    return nodeID & 0x1 == 1 && nodeID < (1 << tree.Depth())
}

// this
func (tree *MerkleTree) Update(update *dbobject.Update) (map[uint32]bool, map[uint32]map[string]dbobject.Hash) {
    tree.updateLock.Lock()
    
    defer tree.updateLock.Unlock()
    
    modifiedNodes := make(map[uint32]bool)
    objectHashes := make(map[uint32]map[string]dbobject.Hash)
    nodeQueue := NewQueue(uint32(update.Size()))
    
    // should return a set of changes that should be persisted
    for diff := range update.Iter() {
        key := diff.Key()
        keyHash := dbobject.NewHash([]byte(key))
        newObjectHash := diff.NewSiblingSet().Hash([]byte(key))
        oldObjectHash := diff.OldSiblingSet().Hash([]byte(key))
        leaf := LeafNode(&keyHash, tree.depth)
        
        if _, ok := objectHashes[leaf]; !ok {
            objectHashes[leaf] = make(map[string]dbobject.Hash)
        }
    
        objectHashes[leaf][key] = newObjectHash
        nodeQueue.Enqueue(leaf)
        
        tree.nodes[leaf] = tree.nodes[leaf].Xor(oldObjectHash).Xor(newObjectHash)
    }
    
    for nodeQueue.Size() > 0 {
        node := nodeQueue.Dequeue()
        shift := CountTrailingZeros(node) - 1
        left := node - (1 << shift)
        right := node + (1 << shift)
        
        modifiedNodes[node] = true
        
        // if this it the merkle root node
        if node != (1 << (tree.depth - 1)) {
            nodeQueue.Enqueue(ParentNode(node))
        }
        
        if node & 0x1 != 1 {
            tree.nodes[node] = tree.nodes[left].Xor(tree.nodes[right])
        }
    }
    
    return modifiedNodes, objectHashes
}

func LeafNode(keyHash *dbobject.Hash, depth uint8) uint32 {
    // need to force hash value into depth bytes
    // max: 64 - 1:  63 -> normalizedHash: [0, 1]
    // min: 64 - 28: 36 -> normalizedHash: [0, 268435455]
    var shiftAmount uint8 = uint8(unsafe.Sizeof(keyHash.High()))*8 - depth
    var normalizedHash uint32 = uint32(keyHash.High() >> shiftAmount)
    
    return normalizedHash | 0x1
}

func ParentNode(node uint32) uint32 {
    shift := CountTrailingZeros(node)
    var parentOffset uint32 = 1 << shift
    var direction uint32 = (node >> (shift + 1)) & 0x1

    if direction == 1 {
        return node - parentOffset
    }
    
    return node + parentOffset
}

func CountTrailingZeros(n uint32) uint32 {
    var c uint32 = 0
    
    if n & 0x1 == 0 {
        c = 1
        
        if (n & 0xffff) == 0 {
            n >>= 16
            c += 16
        }
        
        if (n & 0xff) == 0 {
            n >>= 8
            c += 8
        }
        
        if (n & 0xf) == 0 {
            n >>= 4
            c += 4
        }
        
        if (n & 0x3) == 0 {
            n >>= 2
            c += 2
        }
        
        c -= n & 0x1
    }
    
    return c
}

func abs(v int32) uint32 {
    if v < 0 {
        return uint32(v*-1)
    }
    
    return uint32(v)
}

type queue struct {
    q []uint32
    head uint32
    size uint32
}

func NewQueue(capacity uint32) *queue {
    return &queue{ make([]uint32, capacity), 0, 0 }
}

func (q *queue) Size() uint32 {
    return q.size
}

func (q *queue) Enqueue(n uint32) {
    if q.size == uint32(len(q.q)) {
        return
    }

    i := q.head + q.size
    
    if i >= uint32(len(q.q)) {
        i = i - uint32(len(q.q))
    }
    
    q.q[i] = n
    q.size += 1
}

func (q *queue) Dequeue() uint32 {
    if q.size == 0 {
        return 0
    }

    n := q.q[q.head]
    
    q.head += 1
    q.size -= 1

    if q.head >= q.size {
        q.head = 0
    }
    
    return n
}
