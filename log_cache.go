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
		cache:        make(map[string]*cacheItem),
		cacheOnWrite: options.CacheOnWrite,
		store:        options.Store,
		ttl:          ttl,
	}
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
			lc.cache[fmt.Sprintf("%d", entry.Index)] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: entry}
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
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.(*LogEntry), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data, err := lc.store.GetLogByIndex(index)
	if err != nil {
		return data, err
	}

	lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// GetLogsByRange return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetLogsByRange(minIndex, maxIndex, maxAppendEntries uint64) (response GetLogsByRangeResponse) {
	key := fmt.Sprintf("%d%d%d", minIndex, maxIndex, maxAppendEntries)

	lc.mu.RLock()
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.(GetLogsByRangeResponse)
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data := lc.store.GetLogsByRange(minIndex, maxIndex, maxAppendEntries)
	if data.Err == nil {
		lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}
	}

	return data
}

// GetLastConfiguration return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetLastConfiguration(key string) (*LogEntry, error) {
	lc.mu.RLock()
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.(*LogEntry), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data, err := lc.store.GetLastConfiguration()
	if err != nil {
		return data, err
	}

	lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// DiscardLogs remove key cache and from long term storage
func (lc *LogCache) DiscardLogs(minIndex, maxIndex uint64) error {
	lc.mu.Lock()
	for index := minIndex; index <= maxIndex; index++ {
		delete(lc.cache, fmt.Sprintf("%d", index))
	}
	lc.mu.Unlock()

	return lc.store.DiscardLogs(minIndex, maxIndex)
}

// GetMetadata return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) GetMetadata(key string) ([]byte, error) {
	lc.mu.RLock()
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.([]byte), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data, err := lc.store.GetMetadata()
	if err != nil {
		return data, err
	}

	lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// storeMetadata stores data in cache and in long term storage
func (lc *LogCache) storeMetadata(key string, value []byte) error {
	lc.mu.Lock()
	if lc.cacheOnWrite {
		lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: value}
	}
	lc.mu.Unlock()

	return lc.store.StoreMetadata(value)
}

// Set stores data in cache and in long term storage
func (lc *LogCache) Set(key, value []byte) error {
	lc.mu.Lock()
	if lc.cacheOnWrite {
		lc.cache[string(key)] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: value}
	}
	lc.mu.Unlock()

	return lc.store.Set(key, value)
}

// Get return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) Get(key []byte) ([]byte, error) {
	lc.mu.RLock()
	s := string(key)
	if val, ok := lc.cache[s]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.([]byte), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, s)

	data, err := lc.store.Get(key)
	if err != nil {
		return data, err
	}

	if lc.cacheOnWrite {
		lc.cache[s] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}
	}

	return data, err
}

// FirstIndex return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) FirstIndex(key string) (uint64, error) {
	lc.mu.RLock()
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.(uint64), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data, err := lc.store.FirstIndex()
	if err != nil {
		return data, err
	}

	lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}

// LastIndex return data from cache when exist otherwise
// data is fetch from long term storage
func (lc *LogCache) LastIndex(key string) (uint64, error) {
	lc.mu.RLock()
	if val, ok := lc.cache[key]; ok {
		// if key is not expired
		if !val.isExpired() {
			lc.mu.RUnlock()
			return val.data.(uint64), nil
		}
	}
	lc.mu.RUnlock()

	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cache, key)

	data, err := lc.store.LastIndex()
	if err != nil {
		return data, err
	}

	lc.cache[key] = &cacheItem{ttl: time.Now().Add(lc.ttl), data: data}

	return data, err
}
