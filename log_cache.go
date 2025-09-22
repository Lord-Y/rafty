package rafty

import (
	"fmt"
	"time"
)

// NewLogCache allow us to configure the cache with the provided store option
func NewLogCache(options LogCacheOptions) *LogCache {
	var ttl time.Duration
	if options.TTL == 0 {
		ttl = 30 * time.Second
	} else {
		ttl = options.TTL
	}

	return &LogCache{
		logs:         make(map[string]*cacheItem),
		metadata:     make(map[string]*cacheItem),
		kv:           make(map[string]*cacheItem),
		cacheOnWrite: options.CacheOnWrite,
		store:        options.Store,
		ttl:          ttl,
	}
}

// Close will close the underlying long term store of the cache
func (lc *LogCache) Close() error {
	lc.logs = nil
	lc.metadata = nil
	lc.kv = nil
	return lc.store.Close()
}

// isExpired return true if the cacheItem is expired
func (i *cacheItem) isExpired() bool {
	return !i.ttl.IsZero() && time.Now().Before(i.ttl)
}

// StoreLogs stores multiple log entries in cache but also
// in long term storage
func (lc *LogCache) StoreLogs(logs []*LogEntry) error {
	if lc.cacheOnWrite {
		lc.mu.Lock()
		for _, entry := range logs {
			lc.logs[fmt.Sprintf("%d", entry.Index)] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: entry}
		}
		lc.mu.Unlock()
	}

	return lc.store.StoreLogs(logs)
}

// StoreLogs stores multiple log entries
func (lc *LogCache) StoreLog(log *LogEntry) error {
	return lc.StoreLogs([]*LogEntry{log})
}

// GetLogByIndex return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetLogByIndex(index uint64) (*LogEntry, error) {
	key := fmt.Sprintf("%d", index)

	lc.mu.RLock()
	if val, ok := lc.logs[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(*LogEntry), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.logs, key)

	data, err := lc.store.GetLogByIndex(index)
	if err != nil {
		return data, err
	}

	lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// GetLogsByRange return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetLogsByRange(minIndex, maxIndex, maxAppendEntries uint64) (response GetLogsByRangeResponse) {
	key := fmt.Sprintf("%d%d%d", minIndex, maxIndex, maxAppendEntries)

	lc.mu.RLock()
	if val, ok := lc.logs[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(GetLogsByRangeResponse)
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.logs, key)

	data := lc.store.GetLogsByRange(minIndex, maxIndex, maxAppendEntries)
	if data.Err == nil {
		lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}
	}

	return data
}

// GetLastConfiguration return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetLastConfiguration() (*LogEntry, error) {
	lc.mu.RLock()
	key := "lastConfiguration"
	if val, ok := lc.logs[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(*LogEntry), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.logs, key)

	data, err := lc.store.GetLastConfiguration()
	if err != nil {
		return data, err
	}

	lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// DiscardLogs remove key cache and from long term storage
func (lc *LogCache) DiscardLogs(minIndex, maxIndex uint64) error {
	lc.mu.Lock()
	for index := minIndex; index <= maxIndex; index++ {
		delete(lc.logs, fmt.Sprintf("%d", index))
	}
	lc.mu.Unlock()

	return lc.store.DiscardLogs(minIndex, maxIndex)
}

// FirstIndex return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) FirstIndex() (uint64, error) {
	key := "rafty_cache_first_index"
	lc.mu.RLock()
	if val, ok := lc.logs[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(uint64), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.logs, key)

	data, err := lc.store.FirstIndex()
	if err != nil {
		return data, err
	}

	lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// LastIndex return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) LastIndex() (uint64, error) {
	lc.mu.RLock()
	key := "rafty_cache_last_index"
	if val, ok := lc.logs[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(uint64), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.logs, key)

	data, err := lc.store.LastIndex()
	if err != nil {
		return data, err
	}

	lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// GetMetadata return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetMetadata() ([]byte, error) {
	lc.mu.RLock()
	key := "rafty_cache_metadata"
	if val, ok := lc.metadata[key]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.([]byte), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.metadata, key)

	data, err := lc.store.GetMetadata()
	if err != nil {
		return data, err
	}

	lc.metadata[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// StoreMetadata stores data in cache and in long term storage
func (lc *LogCache) StoreMetadata(value []byte) error {
	lc.mu.Lock()
	key := "rafty_cache_metadata"
	if lc.cacheOnWrite {
		lc.metadata[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: value}
	}
	lc.mu.Unlock()

	return lc.store.StoreMetadata(value)
}

// Set stores data in cache and in long term storage
func (lc *LogCache) Set(key, value []byte) error {
	lc.mu.Lock()
	if lc.cacheOnWrite {
		lc.kv[string(key)] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: value}
	}
	lc.mu.Unlock()

	return lc.store.Set(key, value)
}

// Get return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) Get(key []byte) ([]byte, error) {
	lc.mu.RLock()
	s := string(key)
	if val, ok := lc.kv[s]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.([]byte), nil
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.kv, s)

	data, err := lc.store.Get(key)
	if err != nil {
		return data, err
	}

	if lc.cacheOnWrite {
		lc.kv[s] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}
	}

	return data, err
}

// SetUint64 stores data in cache and in long term storage
func (lc *LogCache) SetUint64(key, value []byte) error {
	lc.mu.Lock()
	if lc.cacheOnWrite {
		lc.kv[string(key)] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: value}
	}
	lc.mu.Unlock()

	return lc.store.SetUint64(key, value)
}

// GetUint64 return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetUint64(key []byte) uint64 {
	lc.mu.RLock()
	s := string(key)
	if val, ok := lc.kv[s]; ok && !val.isExpired() {
		lc.mu.RUnlock()
		return val.data.(uint64)
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.kv, s)

	data := lc.store.GetUint64(key)

	if lc.cacheOnWrite {
		lc.kv[s] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}
	}

	return data
}
