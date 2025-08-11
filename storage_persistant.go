package rafty

// Store is an interface that allow us to store and retrieve
// logs from memory
type Store interface {
	// Close permits to close the store
	Close() error

	// StoreLogs stores multiple log entries
	StoreLogs(logs []*logEntry) error

	// StoreLog stores a single log entry
	StoreLog(log *logEntry) error

	// GetLogByIndex permits to retrieve log from specified index
	GetLogByIndex(index uint64) (*logEntry, error)

	// GetLogsByRange will return a slice of logs
	// with peer lastLogIndex and leader lastLogIndex capped
	// by options.MaxAppendEntries
	GetLogsByRange(peerLastLogIndex, leaderLastLogIndex, maxAppendEntries uint64) GetLogsByRangeResponse

	// GetLastConfiguration returns the last configuration found
	// in logs
	GetLastConfiguration() (*logEntry, error)

	// DiscardLogs permits to wipe entries with the provided range indexes
	DiscardLogs(from, to uint64) error

	// FistIndex will return the first index from the raft log
	FirstIndex() (uint64, error)

	// LastIndex will return the last index from the raft log
	LastIndex() (uint64, error)

	// GetMetadata will fetch rafty metadata from the k/v store
	GetMetadata() ([]byte, error)

	// storeMetadata will store rafty metadata into the k/v bucket.
	// This won't be replicated
	storeMetadata([]byte) error

	// Set will add key/value to the k/v store
	Set(key, value []byte) error

	// Get will fetch provided key from the k/v store
	Get(key []byte) ([]byte, error)

	// SetUint64 will add key/value to the k/v store
	SetUint64(key, value []byte) error

	// GetUint64 will fetch provided key from the k/v store
	GetUint64(key []byte) uint64
}

// GetLogsByRangeResponse returns response from GetLogsByRange func
type GetLogsByRangeResponse struct {
	Logs                             []*logEntry
	Total, LastLogIndex, LastLogTerm uint64
	SendSnapshot                     bool
	Err                              error
}
