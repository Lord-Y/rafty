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
	// Store hold the long term storage data
	Store Store

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

	// cache hold the log entry
	cache map[string]*cacheItem

	// cacheOnWrite when set to true will put every write
	// request in cache before writting to the long term storage
	cacheOnWrite bool

	// store hold the long term storage data
	store Store

	// ttl is the maximum amount of time to keep the entry in cache.
	// Default to 30 seconds if ttl == 0
	ttl time.Duration
}
