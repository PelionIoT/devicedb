package crstrategies

import (
    "devicedb/dbobject"
)

func LastWriterWins(siblingSet *dbobject.SiblingSet) *dbobject.SiblingSet {
    var newestSibling *dbobject.Sibling
    
    for sibling := range siblingSet.Iter() {
        if newestSibling == nil || sibling.Timestamp() > newestSibling.Timestamp() {
            newestSibling = sibling
        }
    }
    
    if newestSibling == nil {
        return siblingSet
    }
    
    return dbobject.NewSiblingSet(map[*dbobject.Sibling]bool{ newestSibling: true })
}