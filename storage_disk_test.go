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

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
	}
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()

	makeStorage := func(node *Rafty, only bool) {
		if only {
			node.storage.metadata.rafty = node
			node.storage.data.rafty = node
			return
		}

		metaFile, dataFile := node.newStorage()
		node.storage = storage{
			metadata: metaFile,
			data:     dataFile,
		}
		node.storage.metadata.rafty = node
		node.storage.data.rafty = node
		node.logs = node.newLogs()
	}

	makeNewNode := func(node *Rafty) *Rafty {
		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.id = id
		node.currentTerm.Store(currentTerm)
		node.votedFor = votedFor
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		node.options.PersistDataOnDisk = true
		return node
	}

	t.Run("createDirectoryIfNotExist", func(t *testing.T) {
		assert.Error(createDirectoryIfNotExist("", 0750))
	})

	t.Run("newStorage", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		metadata, data := node.newStorage()
		assert.Equal(metaFile{}, metadata)
		assert.Equal(dataFile{}, data)
	})

	t.Run("restore_close", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "restore_close")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		makeStorage(node, false)
		node.logs.rafty = node
		assert.Nil(node.storage.restore())
		node.storage.close()
	})

	t.Run("restore_metadata", func(t *testing.T) {
		x := basicNodeSetup()
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		makeStorage(node, false)
		node.options.Peers = x.options.Peers
		node.logs.rafty = node
		assert.Nil(node.storage.metadata.restore())
		node.storage.metadata.close()
	})

	t.Run("persist_metadata_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.id = id
		node.currentTerm.Store(currentTerm)
		node.votedFor = votedFor

		err := node.storage.metadata.store()
		assert.Nil(err)

		node.options.PersistDataOnDisk = true
		makeStorage(node, false)
		err = node.storage.metadata.restore()
		assert.Nil(err)

		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_persist_metadata_basic")
		makeStorage(node, false)
		err = node.storage.metadata.restore()
		assert.Nil(err)
		node.storage.metadata.close()

		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_metadata_with_persistence_and_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_metadata_with_persistence_and_filepath")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)

		makeStorage(node, true)
		err := node.storage.metadata.restore()
		assert.Nil(err)
		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_metadata", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		x := basicNodeSetup()
		id, currentTerm := "test", uint64(1)
		node := cc.cluster[0]
		node.Logger = &logger
		node.options.Peers = x.options.Peers
		makeStorage(node, true)

		err := node.storage.metadata.restore()
		assert.Nil(err)
		assert.Equal("", node.id)
		assert.Equal(uint64(0), node.currentTerm.Load())
		node = makeNewNode(node)
		makeStorage(node, false)

		err = node.storage.metadata.restore()
		assert.Nil(err)
		err = node.storage.metadata.store()
		assert.Nil(err)

		err = node.storage.metadata.restore()
		assert.Nil(err)

		assert.Equal(id, node.id)
		assert.Equal(currentTerm, node.currentTerm.Load())
		node.storage.metadata.close()

		node = nil
		cc.cluster = cc.makeCluster()
		node = cc.cluster[0]
		node.Logger = &logger
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		node.options.PersistDataOnDisk = true
		makeStorage(node, false)
		err = node.storage.metadata.restore()
		assert.Nil(err)
		node.storage.metadata.close()

		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)

		node = nil
		cc.cluster = cc.makeCluster()
		node = cc.cluster[0]
		node.Logger = &logger

		node = makeNewNode(node)
		makeStorage(node, false)

		node.options.Peers = x.options.Peers
		err = node.storage.metadata.store()
		assert.Nil(err)
		err = node.storage.metadata.restore()
		assert.Nil(err)
		assert.NotNil(node.configuration.ServerMembers)

		err = node.storage.metadata.restore()
		assert.Nil(err)

		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_data_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		err := node.storage.data.restore()
		assert.Nil(err)
		node.storage.data.close()
	})

	t.Run("restore_data_with_persistence_but_no_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.options.PersistDataOnDisk = true
		makeStorage(node, false)

		err := node.storage.data.restore()
		assert.Nil(err)
		node.storage.data.close()
	})

	t.Run("persist_data_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		makeStorage(node, false)

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err := encodeCommand(userCommand, buffer)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: buffer.Bytes()}
		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		err = node.storage.data.store(&entry)
		assert.Nil(err)

		node.options.PersistDataOnDisk = true
		makeStorage(node, false)

		err = node.storage.data.store(&entry)
		assert.Nil(err)

		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_persist_data_basic")
		makeStorage(node, false)

		err = node.storage.data.store(&entry)
		assert.Nil(err)
		node.storage.data.close()

		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_data_with_persistence_and_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_metadata_with_persistence_and_filepath")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)

		makeStorage(node, false)
		err := node.storage.data.restore()
		assert.Nil(err)
		node.storage.data.close()

		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_data", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		makeStorage(node, false)

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err := encodeCommand(userCommand, buffer)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: buffer.Bytes()}
		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(1, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)

		node.logs.log = nil
		err = node.storage.data.restore()
		assert.Nil(err)
		assert.Equal(1, len(node.logs.log))
		assert.Equal(now, node.logs.log[0].Timestamp)

		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(2, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)
		node.logs.log = nil
		err = node.storage.data.restore()
		assert.Nil(err)
		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.GreaterOrEqual(node.logs.log[0].Index, uint64(0))
		assert.Greater(node.logs.log[1].Index, uint64(0))

		node.storage.data.close()

		node = nil
		cc.cluster = cc.makeCluster()
		node = cc.cluster[0]
		node.Logger = &logger
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")
		node.options.PersistDataOnDisk = true
		err = node.storage.data.restore()
		assert.Nil(err)

		node.storage.data.close()
		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
	})

	t.Run("storeWithEntryIndex_file_nil", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "storeWithEntryIndex")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		node.logs.rafty = node

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err := encodeCommand(userCommand, buffer)
		assert.Nil(err)

		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: buffer.Bytes()}
		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		err = node.storage.data.storeWithEntryIndex(0)
		assert.Nil(err)

		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(2, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)
		node.storage.data.close()
		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
		node.wg.Wait()
	})

	t.Run("storeWithEntryIndex_file_not_nil", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		node.options.PersistDataOnDisk = true
		node.options.DataDir = filepath.Join(os.TempDir(), "storeWithEntryIndex")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		makeStorage(node, false)
		node.logs.rafty = node

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		buffer := new(bytes.Buffer)
		err := encodeCommand(userCommand, buffer)
		assert.Nil(err)

		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: buffer.Bytes()}
		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		err = node.storage.data.storeWithEntryIndex(0)
		assert.Nil(err)

		_ = node.logs.appendEntries([]*raftypb.LogEntry{&entry}, false)
		assert.Equal(2, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)
		node.storage.data.close()
		err = os.RemoveAll(node.options.DataDir)
		assert.Nil(err)
		node.wg.Wait()
	})
}
