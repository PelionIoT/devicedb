package io

/*import (
    "bufio"
    "devicedb/dbobject"
)

type Peer struct {
    connections map[*websocket.Conn]bool
    buckets *BucketList
}

func NewPeer(bucketList *BucketList) *Peer {
    peer := &Peer{
        connections: make(map[*websocket.Conn]bool),
        buckets: bucketList,
    }
}

func (peer *Peer) Accept(connection *websocket.Conn) {
    // accept peer connection, start listening, negotiate handshake
}

func (peer *Peer) Connect(url string) {
    // establish a new connection with another peer
}

func (peer *Peer) register(connection *websocket.Conn) {
    reader := bufio.NewReader(connection)

    for {
        next, err := reader.ReadString('\n')
        
        if err != nil {
            
        }
        
        someChan <- next
    }
}

type SyncController struct {
    buckets *BucketList
}

func NewSyncController(maxSyncSessions uint, bucketList *BucketList) *SyncController {
    syncController := &SyncController{
        idleSyncSessions: make(chan *SyncSession, maxSyncSessions),
        buckets: bucketList,
    }
    
    return syncController
}

func (s *SyncController) Start() {
    go func() {
        for {
            syncSession := <-s.idleSyncSessions
            nextPeer := <-s.syncPeers
        }
    }()
    
    go func() {
        for {
            select {
            case sync := <-s.syncMessages:
            case join := <-s.joins:
                // peer join
            case leave := <-s.leaves:
                // peer leave
            }
        }
    }()
}
*/