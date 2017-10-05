package client

type EntryIterator struct {
}

func (iter *EntryIterator) Next() bool {
    return false
}

func (iter *EntryIterator) Prefix() string {
    return ""
}

func (iter *EntryIterator) Key() string {
    return ""
}

func (iter *EntryIterator) Entry() Entry {
    return Entry{}
}