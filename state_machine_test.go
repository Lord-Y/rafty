package rafty

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// userLogsInMemory holds the requirements related to user data
type userLogsInMemory struct {
	// mu holds locking mecanism
	mu sync.RWMutex

	// userStore map holds a map of the log entries
	userStore map[uint64]*LogEntry

	// metadata map holds a map of metadata store
	metadata map[string][]byte

	// kv map holds a map of k/v store
	kv map[string][]byte
}

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore LogStore

	// userStore is only for user land management
	userStore userLogsInMemory

	applyErrTest error
	sleepErr     time.Duration
}

// CommandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type CommandKind uint32

const (
	// commandGet command allows us to fetch data from the cluster
	CommandGet CommandKind = iota

	// CommandSet command allows us to write data from the cluster
	CommandSet

	// CommandDelete command allows us to delete data from the cluster
	CommandDelete
)

// Command is the struct to use to interact with cluster data
type Command struct {
	// Kind represent the set of commands: get, set, del
	Kind CommandKind

	// Key is the name of the key
	Key string

	// Value is the value associated to the key
	Value string
}

// EncodeCommand permits to transform command receive from clients to binary language machine
func EncodeCommand(cmd Command, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(cmd.Kind)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Key))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Key)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Value))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Value)); err != nil {
		return err
	}
	return nil
}

// DecodeCommand permits to transform back command from binary language machine to clients
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	buffer := bytes.NewBuffer(data)

	var kind uint32
	if err := binary.Read(buffer, binary.LittleEndian, &kind); err != nil {
		return cmd, err
	}
	cmd.Kind = CommandKind(kind)

	var keyLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &keyLen); err != nil {
		return cmd, err
	}

	key := make([]byte, keyLen)
	if _, err := buffer.Read(key); err != nil {
		return cmd, err
	}
	cmd.Key = string(key)

	var valueLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
		return cmd, err
	}
	value := make([]byte, valueLen)
	if _, err := buffer.Read(value); err != nil {
		return cmd, err
	}
	cmd.Value = string(value)

	return cmd, nil
}

// NewSnapshotState return a SnapshotState that allows us to
// take or restore snapshots
func NewSnapshotState(logStore LogStore) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
		userStore: userLogsInMemory{
			userStore: make(map[uint64]*LogEntry),
			metadata:  make(map[string][]byte),
			kv:        make(map[string][]byte),
		},
	}
}

// Snapshot allows us to take snapshots
func (s *SnapshotState) Snapshot(snapshotWriter io.Writer) error {
	var err error
	firstIndex, err := s.logStore.FirstIndex()
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	lastIndex, err := s.logStore.LastIndex()
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	if firstIndex != lastIndex {
		// 64 here will do nothing except telling us a snapshot is needed
		// which we are building
		response := s.logStore.GetLogsByRange(firstIndex, lastIndex, 64)
		if response.Err != nil {
			return err
		}
		for _, LogEntry := range response.Logs {
			var err error
			buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
			if err = MarshalBinary(LogEntry, buffer); err != nil {
				return err
			}
			if err = MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
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

// Restore allows us to restore a snapshot
func (s *SnapshotState) Restore(snapshotReader io.Reader) error {
	var logs []*LogEntry
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

		data, err := UnmarshalBinaryWithChecksum(record)
		if err != nil {
			return err
		}

		if data != nil {
			logs = append(logs, data)
		}
	}

	return s.logStore.StoreLogs(logs)
}

func (s *SnapshotState) ApplyCommand(log *LogEntry) ([]byte, error) {
	if s.applyErrTest != nil {
		return nil, s.applyErrTest
	}

	if s.sleepErr > 0 {
		time.Sleep(s.sleepErr)
		return nil, fmt.Errorf("test induced error after sleeping %s", s.sleepErr)
	}

	if log.Command == nil {
		return nil, nil
	}

	decodedCmd, _ := DecodeCommand(log.Command)
	switch decodedCmd.Kind {
	case CommandSet:
		return nil, s.userStore.Set([]byte(decodedCmd.Key), []byte(decodedCmd.Value))

	case CommandGet:
		value, err := s.userStore.Get([]byte(decodedCmd.Key))
		if err != nil {
			return nil, err
		}
		return value, nil

	case CommandDelete:
		return nil, fmt.Errorf("not implemented yet")
	}
	return nil, nil
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *userLogsInMemory) Set(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogsInMemory) Get(key []byte) ([]byte, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *userLogsInMemory) SetUint64(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogsInMemory) GetUint64(key []byte) uint64 {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return DecodeUint64ToBytes(val)
	}
	return 0
}
