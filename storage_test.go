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

func TestMetadata(t *testing.T) {
	assert := assert.New(t)

	cc := clusterConfig{
		t:           t,
		clusterSize: 1,
	}
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()

	t.Run("restore_metadata_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.restoreMetadata()
	})

	t.Run("restore_metadata_with_persistence_but_no_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.PersistDataOnDisk = true
		node.restoreMetadata()
	})

	t.Run("persist_metadata_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.ID = id
		node.CurrentTerm = currentTerm
		node.votedFor = votedFor

		err := node.persistMetadata()
		assert.Nil(err)

		node.PersistDataOnDisk = true
		err = node.persistMetadata()
		assert.Nil(err)

		node.DataDir = filepath.Join(os.TempDir(), "rafty_persist_metadata_basic")
		err = node.persistMetadata()
		assert.Nil(err)
		err = os.RemoveAll(node.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_metadata_with_persistence_and_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.PersistDataOnDisk = true
		node.DataDir = filepath.Join(os.TempDir(), "rafty_restore_metadata_with_persistence_and_filepath")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)

		node.restoreMetadata()
		err := os.RemoveAll(node.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_metadata", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.restoreMetadata()
		assert.Equal("", node.ID)
		assert.Equal(uint64(0), node.CurrentTerm)

		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.ID = id
		node.CurrentTerm = currentTerm
		node.votedFor = votedFor
		node.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)
		node.PersistDataOnDisk = true

		node.restoreMetadata() // needed after node.DataDir is set
		err := node.persistMetadata()
		assert.Nil(err)

		node.restoreMetadata()

		assert.Equal(id, node.ID)
		assert.Equal(currentTerm, node.CurrentTerm)
		node.closeAllFilesDescriptor()

		node = nil
		cc.cluster = cc.makeCluster()
		node = cc.cluster[0]
		node.Logger = &logger
		node.DataDir = filepath.Join(os.TempDir(), "rafty_TestMetadata")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)
		node.PersistDataOnDisk = true
		node.restoreMetadata()
	})

	t.Run("restore_data_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.restoreData()
	})

	t.Run("restore_data_with_persistence_but_no_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.PersistDataOnDisk = true
		node.restoreData()
	})

	t.Run("persist_data_basic", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		userCommand := command{kind: commandSet, key: "a", value: "b"}
		now := uint32(time.Now().Unix())
		entry := raftypb.LogEntry{Timestamp: now, Term: node.CurrentTerm, Command: node.encodeCommand(userCommand)}
		node.log = append(node.log, &entry)
		err := node.persistData(0)
		assert.Nil(err)

		node.PersistDataOnDisk = true
		err = node.persistData(0)
		assert.Nil(err)

		node.DataDir = filepath.Join(os.TempDir(), "rafty_persist_data_basic")
		err = node.persistData(0)
		assert.Nil(err)
		err = os.RemoveAll(node.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_data_with_persistence_and_filepath", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger

		node.PersistDataOnDisk = true
		node.DataDir = filepath.Join(os.TempDir(), "rafty_restore_metadata_with_persistence_and_filepath")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)

		node.restoreData()
		err := os.RemoveAll(node.DataDir)
		assert.Nil(err)
	})

	t.Run("restore_and_persist_data", func(t *testing.T) {
		cc.cluster = cc.makeCluster()
		node := cc.cluster[0]
		node.Logger = &logger
		node.PersistDataOnDisk = true
		node.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")

		userCommand := command{kind: commandSet, key: "a", value: "b"}
		now := uint32(time.Now().Unix())
		entry := raftypb.LogEntry{Timestamp: now, Term: node.CurrentTerm, Command: node.encodeCommand(userCommand)}
		node.log = append(node.log, &entry)
		err := node.persistData(0)
		assert.Nil(err)

		node.log = nil
		node.restoreData()
		assert.Equal(1, len(node.log))
		assert.Equal(now, node.log[0].Timestamp)

		node.closeAllFilesDescriptor()

		node = nil
		cc.cluster = cc.makeCluster()
		node = cc.cluster[0]
		node.Logger = &logger
		node.DataDir = filepath.Join(os.TempDir(), "rafty_restore_and_persist_data")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)
		node.PersistDataOnDisk = true
		node.restoreData()

		err = os.RemoveAll(node.DataDir)
		assert.Nil(err)
	})
}
