package rafty

import (
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestLogs(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()

	entries := []*raftypb.LogEntry{{Term: 1}}
	_ = s.logs.appendEntries(entries, false)

	t.Run("total", func(t *testing.T) {
		result := s.logs.total()
		assert.Equal(result.total, 1)
	})

	t.Run("all", func(t *testing.T) {
		result := s.logs.all()
		assert.Equal(result.total, 1)
	})

	t.Run("wipeEntries_range_various", func(t *testing.T) {
		result := s.logs.wipeEntries(10, 20)
		assert.Error(result.err)
	})

	t.Run("wipeEntries_range_zero", func(t *testing.T) {
		result := s.logs.wipeEntries(0, 0)
		assert.Nil(result.err)
		_ = s.logs.appendEntries(entries, false)
	})

	t.Run("fromIndex", func(t *testing.T) {
		result := s.logs.fromIndex(10)
		assert.Error(result.err)

		result = s.logs.fromIndex(0)
		assert.Equal(1, result.total)
		assert.Nil(result.err)
	})

	t.Run("fromLastLogParameters_one", func(t *testing.T) {
		resultLastLog := s.logs.fromLastLogParameters(0, 1, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(1, resultLastLog.total)
		assert.Equal(uint64(0), resultLastLog.lastLogIndex)
		assert.Equal(uint64(1), resultLastLog.lastLogTerm)
		assert.Nil(resultLastLog.err)
	})

	t.Run("fromLastLogParameters_zero", func(t *testing.T) {
		resultLastLog := s.logs.fromLastLogParameters(0, 0, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(1, resultLastLog.total)
		assert.Equal(uint64(0), resultLastLog.lastLogIndex)
		assert.Equal(uint64(1), resultLastLog.lastLogTerm)
		assert.Nil(resultLastLog.err)
	})

	t.Run("fromLastLogParameters_find_closest_entry", func(t *testing.T) {
		result := s.logs.wipeEntries(0, 0)
		assert.Equal(0, result.total)
		assert.Nil(result.err)
		term, max := uint64(0), uint64(10)

		for i := range max {
			term = uint64(2 + i)
			entries = append(entries, &raftypb.LogEntry{Term: term})
		}
		total := s.logs.appendEntries(entries, false)
		assert.Equal(len(entries), total)
		s.options.MaxAppendEntries = 1
		resultLastLog := s.logs.fromLastLogParameters(max+10, term, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(0, resultLastLog.total)
		assert.Equal(uint64(0), resultLastLog.lastLogIndex)
		assert.Equal(uint64(0), resultLastLog.lastLogTerm)
		assert.Nil(resultLastLog.err)

		s.options.MaxAppendEntries = maxAppendEntries
		resultLastLog = s.logs.fromLastLogParameters(0, term, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(10, resultLastLog.total)
	})

	t.Run("fromLastLogParameters_found", func(t *testing.T) {
		s.options.MaxAppendEntries = maxAppendEntries
		resultLastLog := s.logs.fromLastLogParameters(4, 5, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(1, resultLastLog.total)
		assert.Equal(uint64(0), resultLastLog.lastLogIndex)
		assert.Equal(uint64(1), resultLastLog.lastLogTerm)
	})

	t.Run("wipeEntries_range_exit", func(t *testing.T) {
		result := s.logs.wipeEntries(0, 5)
		assert.Nil(result.err)
	})

	t.Run("appendEntries_check_index", func(t *testing.T) {
		result := s.logs.wipeEntries(0, 0)
		assert.Nil(result.err)
		max := 5
		for i := range max {
			entries = append(entries, &raftypb.LogEntry{Term: uint64(i)})
		}
		_ = s.logs.appendEntries(entries, false)
		all := s.logs.all()
		for i, entry := range all.logs {
			assert.Equal(uint64(i), entry.Index)
		}
	})

	t.Run("applyConfigEntry", func(t *testing.T) {
		peers, _ := s.getPeers()
		newbie := peer{Address: "127.0.0.1:6000", ID: "xyz"}
		peers = append(peers, newbie)
		encodedPeers := encodePeers(peers)
		assert.NotNil(encodedPeers)

		entry := &raftypb.LogEntry{
			LogType: uint32(logConfiguration),
			Index:   1,
			Term:    1,
			Command: encodedPeers,
		}

		err = s.logs.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))

		s.options.ShutdownOnRemove = true
		// apply the same entry again to make sure we don't update current config
		err = s.logs.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))
		assert.Equal(false, s.shutdownOnRemove.Load())

		entry = &raftypb.LogEntry{
			LogType: uint32(logConfiguration),
			Index:   2,
			Term:    1,
			Command: encodedPeers,
		}

		err = s.logs.applyConfigEntry(entry)
		assert.Nil(err)
		assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, newbie))
		assert.Equal(true, s.shutdownOnRemove.Load())

		fakePeer := &raftypb.LogEntry{
			LogType: uint32(logConfiguration),
			Index:   3,
			Term:    1,
			Command: []byte("a=b"),
		}

		err = s.logs.applyConfigEntry(fakePeer)
		assert.NotNil(err)

		entry = &raftypb.LogEntry{
			LogType: uint32(logNoop),
			Term:    1,
		}

		err = s.logs.applyConfigEntry(entry)
		assert.Nil(err)
	})

	s.wg.Wait()
}
