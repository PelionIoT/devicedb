package shared

type ConflictResolutionStrategy func(*SiblingSet) *SiblingSet

func Default(siblingSet *SiblingSet) *SiblingSet {
    return siblingSet
}