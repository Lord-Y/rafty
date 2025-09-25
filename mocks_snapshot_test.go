package rafty

import (
	"bytes"
	"errors"
	"io"
)

type mockSnapshot struct {
	discardCalled int
}

type mockSnapshotter struct {
	snap       *mockSnapshot
	prepareErr error
}

func (m *mockSnapshot) Name() string { return "mock" }
func (m *mockSnapshot) Discard() error {
	m.discardCalled++
	return errors.New("discard error")
}
func (m *mockSnapshot) Close() error { return errors.New("close error") }
func (m *mockSnapshot) Metadata() SnapshotMetadata {
	return SnapshotMetadata{}
}
func (m *mockSnapshot) Read(p []byte) (n int, err error) {
	return 0, errors.New("read error")
}
func (m *mockSnapshot) Reader() (bytes.Buffer, error) {
	var x bytes.Buffer
	return x, errors.New("buffer error")
}

func (m *mockSnapshot) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("write error")
}
func (m *mockSnapshot) Write(p []byte) (n int, err error) {
	return 0, errors.New("seek error")
}

func (m *mockSnapshotter) List() (l []*SnapshotMetadata) {
	return
}

func (m *mockSnapshotter) PrepareSnapshotWriter(lastIncludedIndex, lastIncludedTerm, lastAppliedConfigIndex, lastAppliedConfigTerm uint64, currentConfig Configuration) (Snapshot, error) {
	return m.snap, m.prepareErr
}

func (m *mockSnapshotter) PrepareSnapshotReader(name string) (Snapshot, io.ReadCloser, error) {
	return nil, nil, errors.New("fail to prepare snapshot reader")
}

type mockFSM struct {
	snapshotErr error
}

func (m *mockFSM) Snapshot(w io.Writer) error {
	return m.snapshotErr
}

func (m *mockFSM) Restore(io.Reader) error {
	return m.snapshotErr
}

func (s *mockFSM) ApplyCommand(log *LogEntry) ([]byte, error) {
	return nil, nil
}
