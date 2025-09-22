package rafty

import (
	"maps"
	"slices"
)

func NewInMemoryStorage() *LogInMemory {
	return &LogInMemory{
		memory: make(map[uint64]*LogEntry),
		vars:   make(map[string][]byte),
	}
}

// Close will close database
func (in *LogInMemory) Close() error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.memory = nil
	in.vars = nil

	return nil
}

// StoreLogs stores multiple log entries
func (in *LogInMemory) StoreLogs(logs []*LogEntry) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	for _, entry := range logs {
		in.memory[entry.Index] = entry
	}
	return nil
}

// StoreLog stores a single log entry
func (in *LogInMemory) StoreLog(log *LogEntry) error {
	return in.StoreLogs([]*LogEntry{log})
}

// GetLogByIndex permits to retrieve log from specified index
func (in *LogInMemory) GetLogByIndex(index uint64) (*LogEntry, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.memory[index]; ok {
		return val, nil
	}
	return nil, ErrLogNotFound
}

// GetLogsByRange will return a slice of logs
// with peer lastLogIndex and leader lastLogIndex capped
// by options.MaxAppendEntries
func (in *LogInMemory) GetLogsByRange(minIndex, maxIndex, maxAppendEntries uint64) (response GetLogsByRangeResponse) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	for index := minIndex; index <= maxIndex; index++ {
		if val, ok := in.memory[index]; ok {
			response.Logs = append(response.Logs, val)
			response.Total++
			response.LastLogIndex = val.Index
			response.LastLogTerm = val.Term
			if response.Total+1 > maxAppendEntries {
				response.SendSnapshot = true
			}
		} else {
			return GetLogsByRangeResponse{Err: ErrKeyNotFound}
		}
	}
	return
}

// GetLastConfiguration returns the last configuration found
// in logs
func (in *LogInMemory) GetLastConfiguration() (*LogEntry, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	size := len(in.memory)
	for index := size; index >= 0; index-- {
		if val, ok := in.memory[uint64(index)]; ok && logKind(val.LogType) == LogConfiguration {
			return val, nil
		}
	}
	return nil, ErrLogNotFound
}

// DiscardLogs permits to wipe entries with the provided range indexes
func (in *LogInMemory) DiscardLogs(minIndex, maxIndex uint64) error {
	in.mu.RLock()
	defer in.mu.RUnlock()

	for index := minIndex; index <= maxIndex; index++ {
		if _, ok := in.memory[index]; ok {
			delete(in.memory, index)
		} else {
			return ErrKeyNotFound
		}
	}
	return nil
}

// FirstIndex return fist index from in memory
func (in *LogInMemory) FirstIndex() (uint64, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// in golang and other languages maps which are hashmaps are not ordered
	keys := slices.Sorted(maps.Keys(in.memory))
	for entry := range keys {
		return keys[entry], nil
	}
	return 0, ErrKeyNotFound
}

// LastIndex return last index from in memory
func (in *LogInMemory) LastIndex() (uint64, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// in golang and other languages maps which are hashmaps are not ordered
	keys := slices.Sorted(maps.Keys(in.memory))
	slices.Reverse(keys)
	for entry := range keys {
		return keys[entry], nil
	}
	return 0, ErrKeyNotFound
}

// GetMetadata will fetch rafty metadata from the k/v store
func (in *LogInMemory) GetMetadata() ([]byte, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.vars["rafty_metadata"]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// StoreMetadata will store rafty metadata into the k/v bucket
func (in *LogInMemory) StoreMetadata(value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.vars["rafty_metadata"] = value
	return nil
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *LogInMemory) Set(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.vars[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *LogInMemory) Get(key []byte) ([]byte, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.vars[string(key)]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *LogInMemory) SetUint64(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.vars[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *LogInMemory) GetUint64(key []byte) uint64 {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.vars[string(key)]; ok {
		return DecodeUint64ToBytes(val)
	}
	return 0
}
