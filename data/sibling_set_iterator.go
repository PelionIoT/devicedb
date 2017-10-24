package data

type SiblingSetIterator interface {
    Next() bool
    Prefix() []byte
    Key() []byte
    Value() *SiblingSet
    Release()
    Error() error
}