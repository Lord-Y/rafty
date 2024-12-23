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
	cc.cluster = cc.makeCluster()
	for _, node := range cc.cluster {
		logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()
		node.Logger = &logger

		node.restoreMetadata()
		assert.Equal("", node.ID)
		assert.Equal(uint64(0), node.CurrentTerm)

		id, currentTerm, votedFor := "test", uint64(1), "a"
		node.ID = id
		node.CurrentTerm = currentTerm
		node.votedFor = votedFor
		node.DataDir = filepath.Join(os.TempDir(), "rafty", "storage")
		node.Logger.Debug().Msgf("node.DataDir %s", node.DataDir)

		node.restoreMetadata() // needed after node.DataDir is set
		err := node.persistMetadata()
		assert.Nil(err)

		node.restoreMetadata()

		assert.Equal(id, node.ID)
		assert.Equal(currentTerm, node.CurrentTerm)

		userCommand := command{kind: commandSet, key: "a", value: "b"}
		now := uint32(time.Now().Unix())
		entry := raftypb.LogEntry{TimeStamp: now, Term: node.CurrentTerm, Command: node.encodeCommand(userCommand)}
		node.log = append(node.log, &entry)
		err = node.persistData(0)
		assert.Nil(err)

		node.log = nil
		node.restoreData()
		assert.Equal(1, len(node.log))
		assert.Equal(now, node.log[0].TimeStamp)

		node.closeFileDescriptor(true)
		node.closeFileDescriptor(false)

		err = os.RemoveAll(node.DataDir)
		assert.Nil(err)
	}
}
