package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"maps"
	"slices"

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
			users: make(map[string]data),
			kv:    make(map[string]data),
		},
	}
}

// newSnapshot will return new snapshot config
func newSnapshot(datadir string, maxSnapshot int) rafty.SnapshotStore {
	return rafty.NewSnapshot(datadir, maxSnapshot)
}

// Snapshot allows us to take snapshots
func (f *fsmState) Snapshot(snapshotWriter io.Writer) error {
	f.memoryStore.mu.RLock()
	defer f.memoryStore.mu.RUnlock()

	keys := slices.Sorted(maps.Keys(f.memoryStore.logs))
	for key := range keys {
		var err error
		buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
		if err = rafty.MarshalBinary(f.memoryStore.logs[keys[key]], buffer); err != nil {
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

// Restore allows us to restore a snapshot
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

// ApplyCommand allows us to apply a command to the state machine.
func (f *fsmState) ApplyCommand(log *rafty.LogEntry) ([]byte, error) {
	if rafty.LogKind(log.LogType) == rafty.LogNoop || rafty.LogKind(log.LogType) == rafty.LogConfiguration {
		return f.memoryStore.logsApplyCommand(log)
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
