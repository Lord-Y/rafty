package rafty

import (
	"bytes"
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

func NewSnapshotState(logStore Store) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
	}
}

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
		data := new(bytes.Buffer)
		for _, logEntry := range response.Logs {
			var err error
			buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
			if err = MarshalBinary(logEntry, buffer); err != nil {
				return err
			}

			if err = MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
				return err
			}
			if _, err = data.Write(bufferChecksum.Bytes()); err != nil {
				return err
			}
		}
		// writting data to the file handler
		if _, err = snapshotWriter.Write(data.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (s *SnapshotState) Restore(snapshotReader io.Reader) error {
	return nil
}
