package cloud

import (
    "fmt"

    "github.com/coreos/etcd/raft/raftpb"
)

func asdf() {
    c := raftpb.ConfChange{ }
    
    fmt.Println(c)
}