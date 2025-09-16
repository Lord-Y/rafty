package rafty

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore Store

	applyErrTest error
	sleepErr     time.Duration
}

// CommandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type CommandKind uint32

const (
	// commandGet command allow us to fetch data from the cluster
	CommandGet CommandKind = iota

	// CommandSet command allow us to write data from the cluster
	CommandSet

	// CommandDelete command allow us to delete data from the cluster
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

// NewSnapshotState return a SnapshotState that allow us to
// take or restore snapshots
func NewSnapshotState(logStore Store) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
	}
}

// Snapshot allow us to take snapshots
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

// Restore allow us to restore a snapshot
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

func (s *SnapshotState) ApplyCommand(cmd []byte) ([]byte, error) {
	if s.applyErrTest != nil {
		return nil, s.applyErrTest
	}

	if s.sleepErr > 0 {
		time.Sleep(s.sleepErr)
		return nil, fmt.Errorf("test induced error after sleeping %s", s.sleepErr)
	}

	if cmd == nil {
		return nil, nil
	}

	decodedCmd, _ := DecodeCommand(cmd)
	switch decodedCmd.Kind {
	case CommandSet:
		return nil, s.logStore.Set([]byte(decodedCmd.Key), []byte(decodedCmd.Value))

	case CommandGet:
		value, err := s.logStore.Get([]byte(decodedCmd.Key))
		if err != nil {
			return nil, err
		}
		return value, nil

	case CommandDelete:
		return nil, fmt.Errorf("not implemented yet")
	}
	return nil, nil
}
