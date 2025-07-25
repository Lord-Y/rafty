package rafty

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestLogReplication_SendCatchupAppendEntries(t *testing.T) {
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
			totalFollowers:             uint64(totalFollowers),
			quorum:                     uint64(s.quorum()),
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			catchup:                    true,
			rpcTimeout:                 s.randomRPCTimeout(true),
			membershipChangeInProgress: &atomic.Bool{},
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
			totalFollowers:             uint64(totalFollowers),
			quorum:                     uint64(s.quorum()),
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			catchup:                    true,
			rpcTimeout:                 s.randomRPCTimeout(true),
			membershipChangeInProgress: &atomic.Bool{},
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

func TestLogReplication_singleServerCluster(t *testing.T) {
	assert := assert.New(t)

	t.Run("single_server_append_entries_quit_context", func(t *testing.T) {
		s := singleServerClusterSetup("")
		s.isRunning.Store(true)
		s.State = Leader
		s.options.IsSingleServerCluster = true
		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_singleServerAppendEntries")
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		totalLogs := s.logs.appendEntries(entries, false)
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			membershipChangeInProgress: &atomic.Bool{},
			replyToClient:              true,
			replyToClientChan:          make(chan appendEntriesResponse), // use unbuffered chan on purpose to make test fail
		}

		state.singleServerAppendEntries(request)
		select {
		case <-request.replyToClientChan:
			t.Error("Should not receive any response")
		default:
			// expected
		}
	})

	t.Run("single_server_append_timeout", func(t *testing.T) {
		s := singleServerClusterSetup("")
		s.isRunning.Store(true)
		s.State = Leader
		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_singleServerAppendEntries")
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		totalLogs := s.logs.appendEntries(entries, false)
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			membershipChangeInProgress: &atomic.Bool{},
			replyToClient:              true,
			replyToClientChan:          make(chan appendEntriesResponse), // must be unbuffered
		}

		start := time.Now()
		state.singleServerAppendEntries(request)
		elapsed := time.Since(start)

		if elapsed < 500*time.Millisecond {
			t.Errorf("expected at least 500ms got %v", elapsed)
		}

		select {
		case <-request.replyToClientChan:
			t.Error("Should not receive any response")
		default:
			// expected
		}
	})

	t.Run("reply_to_chan", func(t *testing.T) {
		s := singleServerClusterSetup("")
		s.isRunning.Store(true)
		s.State = Leader
		s.options.PersistDataOnDisk = true
		s.options.DataDir = filepath.Join(os.TempDir(), "rafty_singleServerAppendEntries")
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		totalLogs := s.logs.appendEntries(entries, false)
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			membershipChangeInProgress: &atomic.Bool{},
			replyToClient:              true,
			replyToClientChan:          make(chan appendEntriesResponse, 1),
		}

		state.singleServerAppendEntries(request)
		data := <-request.replyToClientChan
		assert.Nil(data.Error)
	})
}
