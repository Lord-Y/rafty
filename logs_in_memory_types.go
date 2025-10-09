package rafty

import (
	"sync"
)

// LogsInMemory holds the requirements related to in memory rafty data
type LogsInMemory struct {
	// mu holds locking mecanism
	mu sync.RWMutex

	// logs map holds a map of the log entries
	logs map[uint64]*LogEntry
}
