package rafty

import (
	"sync"
)

// LogInMemory hold the requirements related to in memory rafty data
type LogInMemory struct {
	// mu hold locking mecanism
	mu sync.RWMutex

	// memory hold a map of the log entries
	memory map[uint64]*LogEntry

	// vars hold the a map of k/v store
	vars map[string][]byte
}
