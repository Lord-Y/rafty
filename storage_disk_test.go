package rafty

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestStorageDisk(t *testing.T) {
	assert := assert.New(t)
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()

	// makeStorage is an helper when only set to true
	// will only initialize s.storage.metadata.rafty and s.storage.data.rafty
	makeStorage := func(s *Rafty, only bool) {
		if only {
			s.storage.metadata.rafty = s
			s.storage.data.rafty = s
			return
		}

		metaFile, dataFile, err := s.newStorage()
		assert.Nil(err)
		s.storage = storage{
			metadata: metaFile,
			data:     dataFile,
		}
		s.storage.metadata.rafty = s
		s.storage.data.rafty = s
		s.logs = s.newLogs()
	}

	makeNewNode := func(s *Rafty) *Rafty {
		id, currentTerm, votedFor := "test", uint64(1), "a"
		s.id = id
		s.currentTerm.Store(currentTerm)
		s.votedFor = votedFor
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		s.options.PersistDataOnDisk = true
		return s
	}

	t.Run("createDirectoryIfNotExist", func(t *testing.T) {
		assert.Error(createDirectoryIfNotExist("", 0750))
	})

	t.Run("newStorage_success", func(t *testing.T) {
		s := &Rafty{}
		s.Logger = &logger

		metadata, data, err := s.newStorage()
		assert.Nil(err)
		assert.Equal(metaFile{}, metadata)
		assert.Equal(dataFile{}, data)
	})

	t.Run("newStorage_error", func(t *testing.T) {
		s := &Rafty{}
		s.Logger = &logger

		_, _, err := s.newStorage()
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		_, _, err = s.newStorage()
		assert.Nil(err)

		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_newStorage_error")
		_, _, err = s.newStorage()
		assert.Nil(err)

		_ = os.RemoveAll(s.options.DataDir)
		_ = os.MkdirAll(s.options.DataDir, 0750)
		tmpFile := filepath.Join(s.options.DataDir, dataStateDir)
		_, err = os.Create(tmpFile)
		assert.Nil(err)
		_, _, err = s.newStorage()
		assert.Error(err)
		_ = os.RemoveAll(s.options.DataDir)

		tmpFile = filepath.Join(s.options.DataDir, metadataFile)
		_ = os.MkdirAll(tmpFile, 0750)
		_, _, err = s.newStorage()
		assert.Error(err)
		_ = os.RemoveAll(s.options.DataDir)

		tmpFile = filepath.Join(s.options.DataDir, dataStateDir, dataStateFile)
		_ = os.MkdirAll(tmpFile, 0750)
		_, _, err = s.newStorage()
		assert.Error(err)
		_ = os.RemoveAll(s.options.DataDir)

		_, _, err = s.newStorage()
		assert.Nil(err)
		_ = os.RemoveAll(s.options.DataDir)
	})

	t.Run("restore_error", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		metadata, data, err := s.newStorage()
		assert.Nil(err)
		tmpFile, err := os.CreateTemp("", "invalid_metadata")
		assert.Nil(err)
		_, _ = tmpFile.WriteString("{invalid json")
		_, _ = tmpFile.Seek(0, 0)

		metadata.fullFilename = tmpFile.Name()
		metadata.file = tmpFile
		assert.Error(metadata.restore())

		// fake json
		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_newStorage_error")
		_, _, err = s.newStorage()
		assert.Nil(err)

		file := filepath.Join(s.options.DataDir, metadataFile)
		tmpFile, err = os.Create(file)
		assert.Nil(err)
		_, _ = tmpFile.WriteString("{invalid json")
		_, _ = tmpFile.Seek(0, 0)

		metadata.fullFilename = tmpFile.Name()
		metadata.file = tmpFile
		assert.Error(metadata.restore())

		data.fullFilename = tmpFile.Name()
		data.file = tmpFile
		assert.Error(data.restore())

		storage := storage{
			metadata: metadata,
		}
		assert.Error(storage.restore())

		storage.metadata = metaFile{rafty: s, file: nil}
		storage.data = data
		assert.Error(storage.restore())

		storage.data = dataFile{rafty: s, file: nil}
		assert.Nil(storage.restore())
		_ = os.RemoveAll(s.options.DataDir)
	})

	t.Run("restore_close", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "restore_close")
		assert.Nil(s.storage.restore())
		s.storage.close()
	})

	t.Run("persist_metadata_basic", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		id, currentTerm, votedFor := "test", uint64(1), "a"
		s.id = id
		s.currentTerm.Store(currentTerm)
		s.votedFor = votedFor

		err = s.storage.metadata.store()
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		err = s.storage.metadata.restore()
		assert.Nil(err)

		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_persist_metadata_basic")
		err = s.storage.metadata.restore()
		assert.Nil(err)
		s.storage.metadata.close()

		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_metadata_with_persistence_and_filepath", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_metadata_with_persistence_and_filepath")
		makeStorage(s, false)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_metadata", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		makeStorage(s, true)
		currentTerm := uint64(1)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		assert.Equal(uint64(0), s.currentTerm.Load())
		err = s.storage.metadata.store()
		assert.Nil(err)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		s.storage.metadata.close()

		s = nil
		s = basicNodeSetup()
		err = s.parsePeers()
		assert.Nil(err)

		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		s.options.PersistDataOnDisk = true
		makeStorage(s, false)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		s.storage.metadata.close()

		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)

		s = nil
		s = basicNodeSetup()
		err = s.parsePeers()
		assert.Nil(err)

		s = makeNewNode(s)
		makeStorage(s, false)

		err = s.storage.metadata.store()
		assert.Nil(err)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		assert.Equal(currentTerm, s.currentTerm.Load())
		assert.NotNil(s.configuration.ServerMembers)

		s.configuration.ServerMembers = nil
		err = s.storage.metadata.store()
		assert.Nil(err)
		err = s.storage.metadata.restore()
		assert.Nil(err)

		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)
	})

	t.Run("persist_data_basic", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err = encodeCommand(userCommand, buffer)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: s.currentTerm.Load(), Command: buffer.Bytes()}
		_ = s.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		err = s.storage.data.store(&entry)
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		err = s.storage.data.store(&entry)
		assert.Nil(err)

		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_persist_data_basic")
		err = s.storage.data.store(&entry)
		assert.Nil(err)
		s.storage.data.close()

		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_data", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")
		makeStorage(s, false)

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err = encodeCommand(userCommand, buffer)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: s.currentTerm.Load(), Command: buffer.Bytes()}
		_ = s.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(1, len(s.logs.log))
		err = s.storage.data.store(&entry)
		assert.Nil(err)

		s.logs.log = nil
		err = s.storage.data.restore()
		assert.Nil(err)
		assert.Equal(1, len(s.logs.log))
		assert.Equal(now, s.logs.log[0].Timestamp)

		_ = s.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(2, len(s.logs.log))
		err = s.storage.data.store(&entry)
		assert.Nil(err)
		s.logs.log = nil
		err = s.storage.data.restore()
		assert.Nil(err)
		_ = s.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.GreaterOrEqual(s.logs.log[0].Index, uint64(0))
		assert.Greater(s.logs.log[1].Index, uint64(0))
		s.options.BootstrapCluster = true
		s.timer = time.NewTicker(s.randomElectionTimeout())
		peers, _ := s.getAllPeers()
		encoded := encodePeers(peers)
		entryConfig := &raftypb.LogEntry{LogType: uint32(logConfiguration), Timestamp: now, Term: s.currentTerm.Load(), Command: encoded}
		_ = s.logs.appendEntries([]*raftypb.LogEntry{entryConfig}, false)
		err = s.logs.applyConfigEntry(entryConfig)
		assert.Nil(err)
		err = s.storage.metadata.store()
		assert.Nil(err)
		err = s.storage.data.store(entryConfig)
		assert.Nil(err)
		s.storage.data.close()

		s = nil
		s = basicNodeSetup()
		err = s.parsePeers()
		assert.Nil(err)
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")
		s.options.PersistDataOnDisk = true
		s.options.BootstrapCluster = true
		makeStorage(s, false)
		err = s.storage.metadata.restore()
		assert.Nil(err)
		err = s.storage.data.restore()
		assert.Nil(err)
		assert.Greater(s.lastAppliedConfigIndex.Load(), uint64(0))

		s.storage.data.close()
		err = os.RemoveAll(s.options.DataDir)
		assert.Nil(err)
	})
}
