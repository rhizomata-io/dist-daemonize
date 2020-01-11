package kv

import "github.com/golang/protobuf/proto"

// EventType Watch event type
type EventType int32

const (
	//PUT put event
	PUT EventType = 0
	//DELETE delete event
	DELETE EventType = 1
)

//EventTypeNames eventType names
var EventTypeNames = map[int32]string{
	0: "PUT",
	1: "DELETE",
}

// ParseType int32 to EventType
func ParseType(val int32) (x EventType) {
	return EventType(val)
}

func (x EventType) String() string {
	return proto.EnumName(EventTypeNames, int32(x))
}

// DataHandler data handler for GetWithPrefix, GetWithPrefixLimit
type DataHandler func(fullPath string, rowID string, value []byte) bool

// KV ..
type KV interface {
	Close() error
	PutObject(key string, value interface{}) (revision int64, err error)
	Put(key, val string) (revision int64, err error)
	GetOne(key string) (value []byte, err error)
	GetObject(key string, obj interface{}) (err error)
	GetWithPrefix(key string, handler DataHandler) (err error)
	GetWithPrefixLimit(key string, limit int64, handler DataHandler) (err error)
	DeleteOne(key string) (deleted bool, err error)
	DeleteWithPrefix(key string) (deleted int64, err error)
	Watch(key string, handler func(eventType EventType, fullPath string, rowID string, value []byte)) *Watcher
	WatchWithPrefix(key string, handler func(eventType EventType, fullPath string, rowID string, value []byte)) *Watcher
}
