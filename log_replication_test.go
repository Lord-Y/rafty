package rafty

import (
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSendCatchupAppendEntries(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.isRunning.Store(true)
	s.State = Leader
	followers, totalFollowers := s.getPeers()

	entries := []*raftypb.LogEntry{{Term: 1}}
	_ = s.logs.appendEntries(entries, false)

	id := 0
	client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String(), s.configuration.ServerMembers[id].ID)
	currentTerm := s.currentTerm.Add(1)

	t.Run("total_zero", func(t *testing.T) {
		totalLogs := s.logs.appendEntries(entries, false)
		oldRequest := &onAppendEntriesRequest{
			totalFollowers: uint64(totalFollowers),
			quorum:         uint64(s.quorum()),
			term:           currentTerm,
			prevLogIndex:   s.lastLogIndex.Load(),
			prevLogTerm:    s.lastLogTerm.Load(),
			totalLogs:      uint64(totalLogs),
			uuid:           uuid.NewString(),
			commitIndex:    s.commitIndex.Load(),
			entries:        entries,
			catchup:        true,
			rpcTimeout:     s.randomRPCTimeout(true),
		}

		oldResponse := &raftypb.AppendEntryResponse{
			LogNotFound:  true,
			LastLogIndex: 100,
			LastLogTerm:  1,
		}

		followerRepl := &followerReplication{
			peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
	})

	t.Run("timeout", func(t *testing.T) {
		totalLogs := s.logs.appendEntries(entries, false)
		oldRequest := &onAppendEntriesRequest{
			totalFollowers: uint64(totalFollowers),
			quorum:         uint64(s.quorum()),
			term:           currentTerm,
			prevLogIndex:   s.lastLogIndex.Load(),
			prevLogTerm:    s.lastLogTerm.Load(),
			totalLogs:      uint64(totalLogs),
			uuid:           uuid.NewString(),
			commitIndex:    s.commitIndex.Load(),
			entries:        entries,
			catchup:        true,
			rpcTimeout:     s.randomRPCTimeout(true),
		}

		oldResponse := &raftypb.AppendEntryResponse{
			LogNotFound:  true,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}

		followerRepl := &followerReplication{
			peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
	})
	s.wg.Wait()
}
