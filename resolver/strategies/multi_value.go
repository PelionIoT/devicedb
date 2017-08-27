package strategies

import (
    . "devicedb/data"
)

type MultiValue struct {
}

func (mv *MultiValue) ResolveConflicts(siblingSet *SiblingSet) *SiblingSet {
    return siblingSet
}
 