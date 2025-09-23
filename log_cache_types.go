package rafty

import (
	"sync"
	"time"
)

// cacheItem holds the data with its associated ttl
type cacheItem struct {
	// ttl is the maximum amount of time to keep the entry in cache
	ttl time.Time

	// data is the data to put in cache
	data any
}

// LogCacheOptions hold all cache options that will be later
// used by LogCache
type LogCacheOptions struct {
	// LogStore hold the long term storage data related to raft logs
	LogStore LogStore

	// CacheOnWrite when set to true will put every write
	// request in cache before writting to the long term storage
	CacheOnWrite bool

	// TTL is the maximum amount of time to keep the entry in cache.
	// Default to 30 seconds if ttl == 0
	TTL time.Duration
}

// LogCache hold the requirements related to caching rafty data
type LogCache struct {
	// mu hold locking mecanism
	mu sync.RWMutex

	// logs map holds a map of the log entries
	logs map[string]*cacheItem

	// cacheOnWrite when set to true will put every write
	// request in cache before writting to the long term storage
	cacheOnWrite bool

	// logStore hold the long term storage data related to raft logs
	logStore LogStore

	// ttl is the maximum amount of time to keep the entry in cache.
	// Default to 30 seconds if ttl == 0
	ttl time.Duration
}
