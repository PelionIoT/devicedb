package halodb

type DBerror struct {
    message string
    code int
}

func (dbError DBerror) Error() string {
    return dbError.message
}

func (dbError DBerror) Code() int {
    return dbError.code
}

const (
    eEMPTY = iota
    eLENGTH = iota
    eNO_VNODE = iota
    eSTORAGE = iota
)

var (
    EEmpty                 = DBerror{ "Parameter was empty or nil", eEMPTY }
    ELength                = DBerror{ "Parameter is too long", eLENGTH }
    ENoVNode               = DBerror{ "This node does not contain keys in this partition", eNO_VNODE }
    EStorage               = DBerror{ "The storage driver experienced an error", eSTORAGE }
)