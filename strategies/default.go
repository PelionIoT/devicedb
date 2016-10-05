package strategies

import (
    "devicedb/dbobject"
)

type ConflictResolutionStrategy func(*dbobject.SiblingSet) *dbobject.SiblingSet

func Default(siblingSet *dbobject.SiblingSet) *dbobject.SiblingSet {
    return siblingSet
}