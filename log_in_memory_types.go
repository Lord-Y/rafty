package rafty

import (
	"sync"
)

// LogInMemory hold the requirements related to in memory rafty data
type LogInMemory struct {
	// mu hold locking mecanism
	mu sync.RWMutex

	// logs map holds a map of the log entries
	logs map[uint64]*LogEntry

	// metadata map holds the a map of metadata store
	metadata map[string][]byte

	// kv map holds the a map of k/v store
	kv map[string][]byte
}
