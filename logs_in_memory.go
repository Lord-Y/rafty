package rafty

import (
	"maps"
	"slices"
)

func NewLogsInMemory() *LogsInMemory {
	return &LogsInMemory{
		logs: make(map[uint64]*LogEntry),
	}
}

// Close will close database
func (in *LogsInMemory) Close() error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.logs = nil

	return nil
}

// StoreLogs stores multiple log entries
func (in *LogsInMemory) StoreLogs(logs []*LogEntry) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	for _, entry := range logs {
		in.logs[entry.Index] = entry
	}
	return nil
}

// StoreLog stores a single log entry
func (in *LogsInMemory) StoreLog(log *LogEntry) error {
	return in.StoreLogs([]*LogEntry{log})
}

// GetLogByIndex permits to retrieve log from specified index
func (in *LogsInMemory) GetLogByIndex(index uint64) (*LogEntry, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.logs[index]; ok {
		return val, nil
	}
	return nil, ErrLogNotFound
}

// GetLogsByRange will return a slice of logs
// with peer lastLogIndex and leader lastLogIndex capped
// by options.MaxAppendEntries
func (in *LogsInMemory) GetLogsByRange(minIndex, maxIndex, maxAppendEntries uint64) (response GetLogsByRangeResponse) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	for index := minIndex; index <= maxIndex; index++ {
		if val, ok := in.logs[index]; ok {
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
func (in *LogsInMemory) GetLastConfiguration() (*LogEntry, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	keys := slices.Sorted(maps.Keys(in.logs))
	slices.Reverse(keys)
	for index := range keys {
		if val, ok := in.logs[uint64(index)]; ok && LogKind(val.LogType) == LogConfiguration {
			return val, nil
		}
	}
	return nil, ErrLogNotFound
}

// DiscardLogs permits to wipe entries with the provided range indexes
func (in *LogsInMemory) DiscardLogs(minIndex, maxIndex uint64) error {
	in.mu.RLock()
	defer in.mu.RUnlock()

	for index := minIndex; index <= maxIndex; index++ {
		if _, ok := in.logs[index]; ok {
			delete(in.logs, index)
		} else {
			return ErrKeyNotFound
		}
	}
	return nil
}

// CompactLogs permits to wipe all entries lower than the provided index
func (in *LogsInMemory) CompactLogs(index uint64) error {
	in.mu.RLock()
	defer in.mu.RUnlock()

	keys := slices.Sorted(maps.Keys(in.logs))
	for key := range keys {
		if uint64(key) < index {
			delete(in.logs, uint64(key))
		}
	}
	return nil
}

// FirstIndex return fist index from in memory
func (in *LogsInMemory) FirstIndex() (uint64, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// in golang and other languages maps which are hashmaps are not ordered
	keys := slices.Sorted(maps.Keys(in.logs))
	for entry := range keys {
		return keys[entry], nil
	}
	return 0, ErrKeyNotFound
}

// LastIndex return last index from in memory
func (in *LogsInMemory) LastIndex() (uint64, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	// in golang and other languages maps which are hashmaps are not ordered
	keys := slices.Sorted(maps.Keys(in.logs))
	slices.Reverse(keys)
	for entry := range keys {
		return keys[entry], nil
	}
	return 0, ErrKeyNotFound
}
