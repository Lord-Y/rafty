package rafty

import (
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
	}

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
		node := cc.cluster[0]
		node.Logger = &logger
		makeStorage(node, true)

		err := node.storage.metadata.restore()
		assert.Nil(err)
		assert.Equal("", node.id)
		assert.Equal(uint64(0), node.currentTerm.Load())

		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.id = id
		node.currentTerm.Store(currentTerm)
		node.votedFor = votedFor
		node.options.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		node.Logger.Debug().Msgf("node.options.DataDir %s", node.options.DataDir)
		node.options.PersistDataOnDisk = true

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

		userCommand := Command{Kind: CommandSet, Key: "a", Value: "b"}
		now := uint32(time.Now().Unix())
		encoded, err := encodeCommand(userCommand)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: encoded}
		node.logs.log = append(node.logs.log, &entry)
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
		encoded, err := encodeCommand(userCommand)
		assert.Nil(err)
		entry := raftypb.LogEntry{Timestamp: now, Term: node.currentTerm.Load(), Command: encoded}
		node.logs.log = append(node.logs.log, &entry)
		assert.Equal(1, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)

		node.logs.log = nil
		err = node.storage.data.restore()
		assert.Nil(err)
		assert.Equal(1, len(node.logs.log))
		assert.Equal(now, node.logs.log[0].Timestamp)

		node.logs.log = append(node.logs.log, &entry)
		assert.Equal(2, len(node.logs.log))
		err = node.storage.data.store(&entry)
		assert.Nil(err)
		node.logs.log = nil
		err = node.storage.data.restore()
		assert.Nil(err)

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
}
