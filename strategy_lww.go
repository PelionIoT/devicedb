package devicedb

func LastWriterWins(siblingSet *SiblingSet) *SiblingSet {
    var newestSibling *Sibling
    
    for sibling := range siblingSet.Iter() {
        if newestSibling == nil || sibling.Timestamp() > newestSibling.Timestamp() {
            newestSibling = sibling
        }
    }
    
    if newestSibling == nil {
        return siblingSet
    }
    
    return NewSiblingSet(map[*Sibling]bool{ newestSibling: true })
}