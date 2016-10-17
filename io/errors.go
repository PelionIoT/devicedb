package io

import (
    "encoding/json"
)

type DBerror struct {
    Msg string `json:"message"`
    ErrorCode int `json:"code"`
}

func (dbError DBerror) Error() string {
    return dbError.Msg
}

func (dbError DBerror) Code() int {
    return dbError.ErrorCode
}

func (dbError DBerror) JSON() []byte {
    json, _ := json.Marshal(dbError)
    
    return json
}

const (
    eEMPTY = iota
    eLENGTH = iota
    eNO_VNODE = iota
    eSTORAGE = iota
    eINVALID_KEY = iota
    eINVALID_BUCKET = iota
    eINVALID_BATCH = iota
    eMERKLE_RANGE = iota
)

var (
    EEmpty                 = DBerror{ "Parameter was empty or nil", eEMPTY }
    ELength                = DBerror{ "Parameter is too long", eLENGTH }
    ENoVNode               = DBerror{ "This node does not contain keys in this partition", eNO_VNODE }
    EStorage               = DBerror{ "The storage driver experienced an error", eSTORAGE }
    EInvalidKey            = DBerror{ "A key was misformatted", eINVALID_KEY }
    EInvalidBucket         = DBerror{ "An invalid bucket was specified", eINVALID_BUCKET }
    EInvalidBatch          = DBerror{ "An invalid batch was specified", eINVALID_BATCH }
    EMerkleRange           = DBerror{ "An invalid merkle node was requested", eMERKLE_RANGE }
)