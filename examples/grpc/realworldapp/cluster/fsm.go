package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/Lord-Y/rafty"
)

// String return a human readable state of the raft server
func (s commandKind) String() string {
	switch s {
	case kvCommandGetAll:
		return "kvCommandGetAll"
	case kvCommandSet:
		return "kvCommandSet"
	case kvCommandDelete:
		return "kvCommandDelete"
	case userCommandGetAll:
		return "userCommandGetAll"
	case userCommandGet:
		return "userCommandGet"
	case userCommandSet:
		return "userCommandSet"
	case userCommandDelete:
		return "userCommandDelete"
	}
	return "kvCommandGet"
}

// newFSMState will return new fsm state
func newFSMState(logStore rafty.LogStore) *fsmState {
	return &fsmState{
		logStore: logStore,
		memoryStore: memoryStore{
			logs:  make(map[uint64]*rafty.LogEntry),
			users: make(map[string][]byte),
			kv:    make(map[string][]byte),
		},
	}
}

// newSnapshot will return new snapshot config
func newSnapshot(datadir string, maxSnapshot int) rafty.SnapshotStore {
	return rafty.NewSnapshot(datadir, maxSnapshot)
}

// Snapshot allow us to take snapshots
func (f *fsmState) Snapshot(snapshotWriter io.Writer) error {
	var err error
	firstIndex, err := f.logStore.FirstIndex()
	if err != nil && !errors.Is(err, rafty.ErrKeyNotFound) {
		return err
	}

	lastIndex, err := f.logStore.LastIndex()
	if err != nil && !errors.Is(err, rafty.ErrKeyNotFound) {
		return err
	}

	if firstIndex != lastIndex {
		// 64 here will do nothing except telling us a snapshot is needed
		// which we are building
		response := f.logStore.GetLogsByRange(firstIndex, lastIndex, 64)
		if response.Err != nil {
			return err
		}
		for _, logEntry := range response.Logs {
			var err error
			buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
			if err = rafty.MarshalBinary(logEntry, buffer); err != nil {
				return err
			}
			if err = rafty.MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
				return err
			}
			// writting data to the file handler
			if _, err = snapshotWriter.Write(bufferChecksum.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// Restore allow us to restore a snapshot
func (f *fsmState) Restore(snapshotReader io.Reader) error {
	reader := bufio.NewReader(snapshotReader)

	for {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read first 4 bytes to get entry size
		record := make([]byte, length)
		if _, err := io.ReadFull(reader, record); err != nil {
			return err
		}

		data, err := rafty.UnmarshalBinaryWithChecksum(record)
		if err != nil {
			return err
		}
		if _, err := f.ApplyCommand(data); err != nil {
			return err
		}
	}

	return nil
}

// ApplyCommand allow us to apply a command to the state machine.
func (f *fsmState) ApplyCommand(log *rafty.LogEntry) ([]byte, error) {
	if log.Command == nil || rafty.LogKind(log.LogType) == rafty.LogNoop {
		return nil, nil
	}

	kind, err := decodeCommand(log.Command)
	if err != nil {
		return nil, err
	}

	switch kind {
	case userCommandSet, userCommandGet, userCommandGetAll, userCommandDelete:
		return f.userApplyCommand(log)

	case kvCommandSet, kvCommandGet, kvCommandGetAll, kvCommandDelete:
		return f.kvApplyCommand(log)
	}

	return nil, nil
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (m *memoryStore) Set(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (m *memoryStore) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.kv[string(key)]; ok {
		return val, nil
	}
	return nil, rafty.ErrKeyNotFound
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (m *memoryStore) SetUint64(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (m *memoryStore) GetUint64(key []byte) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.kv[string(key)]; ok {
		return rafty.DecodeUint64ToBytes(val)
	}
	return 0
}
