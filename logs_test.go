package rafty

import (
	"os"
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestLogs(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()

	t.Run("string", func(t *testing.T) {

		tests := []LogKind{
			LogNoop,
			LogConfiguration,
			LogReplication,
			LogCommandReadLeaderLease,
			LogCommandLinearizableRead,
		}
		results := []string{
			"logNoop",
			"logConfiguration",
			"logReplication",
			"logCommandReadLeaderLease",
			"logCommandLinearizableRead",
		}

		for k, v := range tests {
			assert.Equal(v.String() == results[k], true)
		}
	})

	t.Run("applyConfigEntry", func(t *testing.T) {
		peers, _ := s.getPeers()
		newbie := Peer{Address: "127.0.0.1:60000", ID: "xyz"}
		peers = append(peers, newbie)
		encodedPeers := EncodePeers(peers)
		assert.NotNil(encodedPeers)
		s.options.BootstrapCluster = true

		entry := &raftypb.LogEntry{
			LogType: uint32(LogConfiguration),
			Index:   1,
			Term:    1,
			Command: encodedPeers,
		}

		err := s.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))

		s.options.ShutdownOnRemove = true
		// apply the same entry again to make sure we don't update current config
		err = s.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))
		assert.Equal(false, s.shutdownOnRemove.Load())

		entry = &raftypb.LogEntry{
			LogType: uint32(LogConfiguration),
			Index:   2,
			Term:    1,
			Command: encodedPeers,
		}

		err = s.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))
		assert.Equal(true, s.shutdownOnRemove.Load())

		fakePeer := &raftypb.LogEntry{
			LogType: uint32(LogConfiguration),
			Index:   3,
			Term:    1,
			Command: []byte("a=b"),
		}

		err = s.applyConfigEntry(fakePeer)
		assert.NotNil(err)

		entry = &raftypb.LogEntry{
			LogType: uint32(LogNoop),
			Term:    1,
		}

		err = s.applyConfigEntry(entry)
		assert.Nil(err)
	})

	s.wg.Wait()
}

func TestLogs_make(t *testing.T) {
	assert := assert.New(t)

	entry := &raftypb.LogEntry{Term: 1}
	result := makeLogEntry(entry)
	assert.Equal(entry.Term, result[0].Term)
	protoResult := makeProtobufLogEntry(result[0])
	assert.Equal(entry.Term, protoResult[0].Term)
}
