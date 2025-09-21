package rafty

import (
	bolt "go.etcd.io/bbolt"
)

const (
	// dbFileName is the name of the database file
	dbFileName string = "rafty.db"
	// bucketLogsName will be used to store rafty logs
	bucketLogsName string = "rafty_logs"
	// bucketMetadataName will be used to store rafty metadata
	bucketMetadataName string = "rafty_metadata"
	// bucketKVName will be used as a simple key/value store
	bucketKVName string = "rafty_kv"
)

type BoltOptions struct {
	// DataDir is the default data directory that will be used to store all data on the disk. It's required
	DataDir string

	// Options hold all bolt options
	Options *bolt.Options
}

type BoltStore struct {
	// dataDir is the default data directory that will be used to store all data on the disk
	// Defaults to os.TempDir()/rafty/db/ ex: /tmp/rafty/db/
	dataDir string

	// db allows us to manipulate the k/v database
	db *bolt.DB
}

// Store is an interface that allow us to store and retrieve
// logs from memory
type Store interface {
	// Close permits to close the store
	Close() error

	// StoreLogs stores multiple log entries
	StoreLogs(logs []*LogEntry) error

	// StoreLog stores a single log entry
	StoreLog(log *LogEntry) error

	// GetLogByIndex permits to retrieve log from specified index
	GetLogByIndex(index uint64) (*LogEntry, error)

	// GetLogsByRange will return a slice of logs
	// with peer lastLogIndex and leader lastLogIndex capped
	// by options.MaxAppendEntries
	GetLogsByRange(peerLastLogIndex, leaderLastLogIndex, maxAppendEntries uint64) GetLogsByRangeResponse

	// GetLastConfiguration returns the last configuration found
	// in logs
	GetLastConfiguration() (*LogEntry, error)

	// DiscardLogs permits to wipe entries with the provided range indexes
	DiscardLogs(from, to uint64) error

	// FistIndex will return the first index from the raft log
	FirstIndex() (uint64, error)

	// LastIndex will return the last index from the raft log
	LastIndex() (uint64, error)

	// GetMetadata will fetch rafty metadata from the k/v store
	GetMetadata() ([]byte, error)

	// StoreMetadata will store rafty metadata into the k/v bucket.
	// This won't be replicated
	StoreMetadata([]byte) error

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
	Logs                             []*LogEntry
	Total, LastLogIndex, LastLogTerm uint64
	SendSnapshot                     bool
	Err                              error
}
