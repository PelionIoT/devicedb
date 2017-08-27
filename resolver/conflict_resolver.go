package resolver

import (
    . "devicedb/data"
)

type ConflictResolver interface {
    ResolveConflicts(*SiblingSet) *SiblingSet
}