package devicedb

type Shared struct {
}

func (shared *Shared) ShouldReplicateOutgoing(peerID string) bool {
    return true
}

func (shared *Shared) ShouldReplicateIncoming(peerID string) bool {
    return true
}

type Server struct {
    bucketList *BucketList
}

func NewServer() *Server {
    server := &Server{ NewBucketList() }
    storageDriver := NewLevelDBStorageDriver("/tmp/testdevicedb", nil)
    nodeID := "nodeA"
    
    defaultNode, _ := NewNode(nodeID, &PrefixedStorageDriver{ []byte{ 0 }, storageDriver }, MerkleDefaultDepth, nil)
    lwwNode, _ := NewNode(nodeID, &PrefixedStorageDriver{ []byte{ 1 }, storageDriver }, MerkleDefaultDepth, lastWriterWins)
    
    server.bucketList.AddBucket("default", defaultNode, &Shared{ })
    server.bucketList.AddBucket("lww", lwwNode, &Shared{ })
    
    return server
}

func (server *Server) Start() error {
    return nil
}

func (server *Server) Stop() error {
    return nil
}