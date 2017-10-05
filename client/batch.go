package client

type Batch struct {
}

func (batch *Batch) Put(key string, value string, context string) *Batch {
	return batch
}

func (batch *Batch) Delete(key string, context string) *Batch {
	return batch
}