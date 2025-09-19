package rafty

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
)

func TestSnapshot_internal(t *testing.T) {
	assert := assert.New(t)

	t.Run("takeSnapshot_none", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		_, err := s.takeSnapshot()
		assert.Error(err)
	})

	t.Run("takeSnapshot_GetLogByIndex_err", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		for index := range 100 {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}
		assert.Nil(s.logStore.Close())

		_, err := s.takeSnapshot()
		assert.Error(err)
	})

	t.Run("takeSnapshot_success_no_config", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		for index := range 100 {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}

		snaphoName, err := s.takeSnapshot()
		assert.Nil(err)
		assert.NotNil(snaphoName)
	})

	t.Run("takeSnapshot_success_with_config", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		for index := range 100 {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}

		peers, _ := s.getAllPeers()
		newbie := Peer{Address: "127.0.0.1:60000", ID: "xyz"}
		peers = append(peers, newbie)
		encodedPeers := EncodePeers(peers)
		assert.NotNil(encodedPeers)
		entries := []*raftypb.LogEntry{
			{
				LogType: uint32(LogConfiguration),
				Term:    1,
				Command: encodedPeers,
			},
		}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		assert.Nil(s.applyConfigEntry(entries[0]))

		snaphoName, err := s.takeSnapshot()
		assert.Nil(err)
		assert.NotNil(snaphoName)
	})

	t.Run("takeSnapshot_snapshotHook", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		snapshotTestHook = func() error { return errors.New("test error") }
		defer func() { snapshotTestHook = nil }()
		_, err := s.takeSnapshot()
		assert.Error(err)
	})

	t.Run("takeSnapshot_discard_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		for index := range 100 {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}

		mockSnap := &mockSnapshot{}
		s.snapshot = &mockSnapshotter{prepareErr: errors.New("prepare error"), snap: mockSnap}

		_, err := s.takeSnapshot()
		assert.Error(err)
		assert.Equal(1, mockSnap.discardCalled)

		// fsm err
		s.snapshot = &mockSnapshotter{prepareErr: nil, snap: mockSnap}
		s.fsm = &mockFSM{snapshotErr: errors.New("fsm snapshot error")}

		_, err = s.takeSnapshot()
		assert.Error(err)
		assert.Equal(2, mockSnap.discardCalled)

		// close err
		s.snapshot = &mockSnapshotter{prepareErr: nil, snap: mockSnap}
		s.fsm = &mockFSM{snapshotErr: nil}

		_, err = s.takeSnapshot()
		assert.Error(err)
		assert.Equal(3, mockSnap.discardCalled)
	})
}
