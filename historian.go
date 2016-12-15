package devicedb

import (
    "encoding/json"
    "encoding/binary"
    "encoding/base64"
    "crypto/rand"
    "fmt"
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
    sources []string
    data *string
    order string
    before uint64
    after uint64
    limit int
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

func (historian *Historian) Query(query *HistoryQuery) {// (*EventIterator, error) {
    var ranges [][2][]byte
    var direction int
    
    if query.order == "asc" {
        direction = FORWARD
    } else {
        direction = BACKWARD
    }
    
    if len(query.sources) == 0 {
        // time -> indexByTime
        ranges = make([][2][]byte, 1)
        
        ranges[0] = [2][]byte{
            (&Event{ Timestamp: query.after }).prefixByTime(),
            (&Event{ Timestamp: query.before + 1 }).prefixByTime(),
        }
    } else if query.data == nil {
        // sources + time -> indexBySourceAndTime
        ranges = make([][2][]byte, 0, len(query.sources))
        
        for _, source := range query.sources {
            ranges = append(ranges, [2][]byte{
                (&Event{ SourceID: source, Timestamp: query.after }).prefixBySourceAndTime(),
                (&Event{ SourceID: source, Timestamp: query.before + 1 }).prefixBySourceAndTime(),
            })
        }
    } else {
        // data + sources + time -> indexByDataSourceAndTime
        ranges = make([][2][]byte, 0, len(query.sources))
        
        for _, source := range query.sources {
            ranges = append(ranges, [2][]byte{
                (&Event{ Data: *query.data, SourceID: source, Timestamp: query.after }).prefixByDataSourceAndTime(),
                (&Event{ Data: *query.data, SourceID: source, Timestamp: query.before + 1 }).prefixByDataSourceAndTime(),
            })
        }
    }
    
    log.Info(direction)
}
