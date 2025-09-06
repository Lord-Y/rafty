package rafty

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore Store
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
