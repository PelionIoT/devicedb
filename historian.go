package devicedb

import (
    "encoding/json"
    "encoding/binary"
    "encoding/base64"
    "crypto/rand"
    "fmt"
    "math"
    "sort"
)

var (
    BY_TIME_PREFIX = []byte{ 0 }
    BY_SOURCE_AND_TIME_PREFIX = []byte{ 1 }
    BY_DATA_SOURCE_AND_TIME_PREFIX = []byte{ 2 }
    DELIMETER = []byte(".")
)

func randomString() string {
    randomBytes := make([]byte, 16)
    rand.Read(randomBytes)
    
    high := binary.BigEndian.Uint64(randomBytes[:8])
    low := binary.BigEndian.Uint64(randomBytes[8:])
    
    return fmt.Sprintf("%05x%05x", high, low)
}

func timestampBytes(ts uint64) []byte {
    bytes := make([]byte, 8)
    
    binary.BigEndian.PutUint64(bytes, ts)
    
    return bytes
}

type HistoryQuery struct {
    Sources []string
    Data *string
    Order string
    Before uint64
    After uint64
    Limit int
}

type Event struct {
    Timestamp uint64 `json:"timestamp"`
    SourceID string `json:"sourceID"`
    Type string `json:"type"`
    Data string `json:"data"`
}

func (event *Event) indexByTime() []byte {
    timestampEncoding := timestampBytes(event.Timestamp)
    uuid := []byte(randomString())
    result := make([]byte, 0, len(BY_TIME_PREFIX) + len(timestampEncoding) + len(DELIMETER) + len(uuid))
    
    result = append(result, BY_TIME_PREFIX...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, uuid...)
    
    return result
}

func (event *Event) prefixByTime() []byte {
    timestampEncoding := timestampBytes(event.Timestamp)
    result := make([]byte, 0, len(BY_TIME_PREFIX) + len(timestampEncoding) + len(DELIMETER))
    
    result = append(result, BY_TIME_PREFIX...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    
    return result
}

func (event *Event) indexBySourceAndTime() []byte {
    sourceEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.SourceID)))
    timestampEncoding := timestampBytes(event.Timestamp)
    uuid := []byte(randomString())
    result := make([]byte, 0, len(BY_SOURCE_AND_TIME_PREFIX) + len(sourceEncoding) + len(DELIMETER) + len(timestampEncoding) + len(DELIMETER) + len(uuid))
    
    result = append(result, BY_SOURCE_AND_TIME_PREFIX...)
    result = append(result, sourceEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, uuid...)
    
    return result
}

func (event *Event) prefixBySourceAndTime() []byte {
    sourceEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.SourceID)))
    timestampEncoding := timestampBytes(event.Timestamp)
    result := make([]byte, 0, len(BY_SOURCE_AND_TIME_PREFIX) + len(sourceEncoding) + len(DELIMETER) + len(timestampEncoding) + len(DELIMETER))
    
    result = append(result, BY_SOURCE_AND_TIME_PREFIX...)
    result = append(result, sourceEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    
    return result
}

func (event *Event) indexByDataSourceAndTime() []byte {
    sourceEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.SourceID)))
    dataEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.Data)))
    timestampEncoding := timestampBytes(event.Timestamp)
    uuid := []byte(randomString())
    result := make([]byte, 0, len(BY_SOURCE_AND_TIME_PREFIX) + len(dataEncoding) + len(DELIMETER) + len(sourceEncoding) + len(DELIMETER) + len(timestampEncoding) + len(DELIMETER) + len(uuid))
    
    result = append(result, BY_DATA_SOURCE_AND_TIME_PREFIX...)
    result = append(result, dataEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, sourceEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, uuid...)
    
    return result
}

func (event *Event) prefixByDataSourceAndTime() []byte {
    sourceEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.SourceID)))
    dataEncoding := []byte(base64.StdEncoding.EncodeToString([]byte(event.Data)))
    timestampEncoding := timestampBytes(event.Timestamp)
    result := make([]byte, 0, len(BY_SOURCE_AND_TIME_PREFIX) + len(dataEncoding) + len(DELIMETER) + len(sourceEncoding) + len(DELIMETER) + len(timestampEncoding) + len(DELIMETER))
    
    result = append(result, BY_DATA_SOURCE_AND_TIME_PREFIX...)
    result = append(result, dataEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, sourceEncoding...)
    result = append(result, DELIMETER...)
    result = append(result, timestampEncoding...)
    result = append(result, DELIMETER...)
    
    return result
}

type Historian struct {
    storageDriver StorageDriver
}

func NewHistorian(storageDriver StorageDriver) *Historian {
    return &Historian{
        storageDriver: storageDriver,
    }
}

func (historian *Historian) LogEvent(event *Event) error {
    // indexed by time
    // indexed by resourceid + time
    // indexed by eventdata + resourceID + time
    batch := NewBatch()
    marshaledEvent, err := json.Marshal(event)
    
    if err != nil {
        log.Errorf("Could not marshal event to JSON: %v", err.Error())
        
        return EStorage
    }
    
    batch.Put(event.indexByTime(), []byte(marshaledEvent))
    batch.Put(event.indexBySourceAndTime(), []byte(marshaledEvent))
    batch.Put(event.indexByDataSourceAndTime(), []byte(marshaledEvent))
    
    err = historian.storageDriver.Batch(batch)
    
    if err != nil {
        log.Errorf("Storage driver error in LogEvent(%v): %s", event, err.Error())
        
        return EStorage
    }
    
    return nil
}

func (historian *Historian) Query(query *HistoryQuery) (*EventIterator, error) {
    var ranges [][2][]byte
    var direction int
    
    if query.Order == "desc" {
        direction = BACKWARD
    } else {
        direction = FORWARD
    }
    
    // query.Before is unspecified so default to max range
    if query.Before == 0 {
        query.Before = math.MaxUint64
    }
    
    // ensure consistent ordering from multiple sources
    sort.Strings(query.Sources)
    
    if len(query.Sources) == 0 {
        // time -> indexByTime
        ranges = make([][2][]byte, 1)
        
        ranges[0] = [2][]byte{
            (&Event{ Timestamp: query.After }).prefixByTime(),
            (&Event{ Timestamp: query.Before }).prefixByTime(),
        }
    } else if query.Data == nil {
        // sources + time -> indexBySourceAndTime
        ranges = make([][2][]byte, 0, len(query.Sources))
        
        for _, source := range query.Sources {
            ranges = append(ranges, [2][]byte{
                (&Event{ SourceID: source, Timestamp: query.After }).prefixBySourceAndTime(),
                (&Event{ SourceID: source, Timestamp: query.Before }).prefixBySourceAndTime(),
            })
        }
    } else {
        // data + sources + time -> indexByDataSourceAndTime
        ranges = make([][2][]byte, 0, len(query.Sources))
        
        for _, source := range query.Sources {
            ranges = append(ranges, [2][]byte{
                (&Event{ Data: *query.Data, SourceID: source, Timestamp: query.After }).prefixByDataSourceAndTime(),
                (&Event{ Data: *query.Data, SourceID: source, Timestamp: query.Before }).prefixByDataSourceAndTime(),
            })
        }
    }
    
    iter, err := historian.storageDriver.GetRanges(ranges, direction)
    
    if err != nil {
        return nil, err
    }
    
    return NewEventIterator(iter, query.Limit), nil
}

type EventIterator struct {
    dbIterator StorageIterator
    parseError error
    currentEvent *Event
    limit uint64
    eventsSeen uint64
}

func NewEventIterator(iterator StorageIterator, limit int) *EventIterator {
    if limit <= 0 {
        limit = 0
    }
    
    return &EventIterator{
        dbIterator: iterator,
        parseError: nil,
        currentEvent: nil,
        limit: uint64(limit),
        eventsSeen: 0,
    }
}

func (ei *EventIterator) Next() bool {
    ei.currentEvent = nil
    
    if !ei.dbIterator.Next() {
        if ei.dbIterator.Error() != nil {
            log.Errorf("Storage driver error in Next(): %s", ei.dbIterator.Error())
        }
        
        return false
    }

    var event Event
    
    ei.parseError = json.Unmarshal(ei.dbIterator.Value(), &event)
    
    if ei.parseError != nil {
        log.Errorf("Storage driver error in Next() key = %v, value = %v: %s", ei.dbIterator.Key(), ei.dbIterator.Value(), ei.parseError.Error())
        
        ei.Release()
        
        return false
    }
    
    ei.currentEvent = &event
    ei.eventsSeen += 1
    
    if ei.limit != 0 && ei.eventsSeen == ei.limit {
        ei.Release()
    }
    
    return true
}

func (ei *EventIterator) Event() *Event {
    return ei.currentEvent
}

func (ei *EventIterator) Release() {
    ei.dbIterator.Release()
}

func (ei *EventIterator) Error() error {
    if ei.parseError != nil {
        return EStorage
    }
        
    if ei.dbIterator.Error() != nil {
        return EStorage
    }
    
    return nil
}
