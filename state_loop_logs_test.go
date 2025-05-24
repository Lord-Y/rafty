package rafty

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestLogsLoop(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	s.wg.Add(1)
	go s.wg.Wait()
	go s.logsLoop()

	entries := []*raftypb.LogEntry{{Term: 1}}
	_ = s.logs.appendEntries(entries)

	t.Run("get_all_logs", func(t *testing.T) {
		result := s.logs.all()
		assert.Equal(result.total, 1)
	})

	t.Run("wipe_index_out_of_range", func(t *testing.T) {
		result := s.logs.wipeEntries(10, 20)
		assert.Error(result.err)
	})

	t.Run("get_from_index_out_of_range", func(t *testing.T) {
		result := s.logs.fromIndex(10)
		assert.Error(result.err)
	})

	t.Run("from_last_log_parameters_limit", func(t *testing.T) {
		result := s.logs.wipeEntries(0, 0)
		assert.Nil(result.err)
		term, max := uint64(0), uint64(10)

		for i := range max {
			term = uint64(2 + i)
			entries = append(entries, &raftypb.LogEntry{Term: term})
		}
		_ = s.logs.appendEntries(entries)
		s.options.MaxAppendEntries = 1
		resultLastLog := s.logs.fromLastLogParameters(max+10, term, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Nil(resultLastLog.err)

		resultLastLog = s.logs.fromLastLogParameters(max, term, s.configuration.ServerMembers[0].Address, s.configuration.ServerMembers[0].ID)
		assert.Equal(resultLastLog.total, 12)
	})
}
