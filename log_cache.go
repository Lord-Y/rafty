package rafty

import (
	"fmt"
	"maps"
	"slices"
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
		cacheOnWrite: options.CacheOnWrite,
		logStore:     options.LogStore,
		ttl:          ttl,
	}
}

// Close will close the underlying long term store of the cache
func (lc *LogCache) Close() error {
	lc.logs = nil
	return lc.logStore.Close()
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

	return lc.logStore.StoreLogs(logs)
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

	data, err := lc.logStore.GetLogByIndex(index)
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

	data := lc.logStore.GetLogsByRange(minIndex, maxIndex, maxAppendEntries)
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

	data, err := lc.logStore.GetLastConfiguration()
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

	return lc.logStore.DiscardLogs(minIndex, maxIndex)
}

// CompactLogs permits to wipe all entries lower than the provided index
func (lc *LogCache) CompactLogs(index uint64) error {
	lc.mu.Lock()
	keys := slices.Sorted(maps.Keys(lc.logs))
	for key := range keys {
		if uint64(key) < index {
			delete(lc.logs, fmt.Sprintf("%d", index))
		}
	}
	lc.mu.Unlock()

	return lc.logStore.CompactLogs(index)
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

	data, err := lc.logStore.FirstIndex()
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

	data, err := lc.logStore.LastIndex()
	if err != nil {
		return data, err
	}

	lc.logs[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}
