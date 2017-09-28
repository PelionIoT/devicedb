package clusterio

type ReadRepairer struct {
    NodeClient NodeClient
}

func NewReadRepairer(nodeClient NodeClient) *ReadRepairer {
    return &ReadRepairer{
        NodeClient: nodeClient,
    }
}

func (readRepairer *ReadRepairer) BeginRepair(readMerger NodeReadMerger) {
}

func (readRepairer *ReadRepairer) StopRepairs() {
}