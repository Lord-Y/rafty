package rafty

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLogReplication_SendCatchupAppendEntries(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	s.isRunning.Store(true)
	s.State = Leader
	followers, totalFollowers := s.getPeers()

	entries := []*raftypb.LogEntry{{Term: 1}}
	s.updateEntriesIndex(entries)
	assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

	id := 0
	client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())
	currentTerm := s.currentTerm.Add(1)

	t.Run("total_zero", func(t *testing.T) {
		oldRequest := &onAppendEntriesRequest{
			totalFollowers:             uint64(totalFollowers),
			quorum:                     uint64(s.quorum()),
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
	})

	t.Run("timeout", func(t *testing.T) {
		oldRequest := &onAppendEntriesRequest{
			totalFollowers:             uint64(totalFollowers),
			quorum:                     uint64(s.quorum()),
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
			Peer:         followers[id],
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
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		s.options.IsSingleServerCluster = true
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(LogNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(LogNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(LogNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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

	t.Run("singleServerAppendEntries_panic", func(t *testing.T) {
		s := singleServerClusterSetup("")

		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		currentTerm := s.currentTerm.Add(1)

		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(LogNoop),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   nil,
			},
		}

		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		assert.Nil(s.logStore.Close())
		request := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
			uuid:                       uuid.NewString(),
			commitIndex:                s.commitIndex.Load(),
			entries:                    entries,
			membershipChangeInProgress: &atomic.Bool{},
			replyToClient:              true,
			replyToClientChan:          make(chan appendEntriesResponse, 1),
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		state.singleServerAppendEntries(request)
	})
}

func TestLogReplication_sendInstallSnapshot(t *testing.T) {
	t.Run("sendSnapshotInProgress", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		entries := []*raftypb.LogEntry{{Term: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendSnapshotInProgress.Store(true)
		followerRepl.sendInstallSnapshot(client)
		s.wg.Wait()
	})

	t.Run("noSnapshotFound", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		entries := []*raftypb.LogEntry{{Term: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendInstallSnapshot(client)
		s.wg.Wait()
	})

	t.Run("errInstallSnapshot", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()

		max := 100
		for index := range max {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendInstallSnapshot(client)
		s.wg.Wait()
	})

	t.Run("response_success_true", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()

		max := 100
		for index := range max {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		id := 0
		mockClient := new(MockRaftyClientTestify)
		mockClient.On("SendInstallSnapshotRequest", mock.Anything, mock.Anything, mock.Anything).
			Return(&raftypb.InstallSnapshotResponse{Success: true}, nil)

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendInstallSnapshot(mockClient)
		s.wg.Wait()

		mockClient.AssertExpectations(t)
	})

	t.Run("response_success_false", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()

		max := 100
		for index := range max {
			var entries []*raftypb.LogEntry
			entries = append(entries, &raftypb.LogEntry{
				Term:    1,
				Command: []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)),
			})

			s.updateEntriesIndex(entries)
			assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		id := 0
		mockClient := new(MockRaftyClientTestify)
		mockClient.On("SendInstallSnapshotRequest", mock.Anything, mock.Anything, mock.Anything).
			Return(&raftypb.InstallSnapshotResponse{Success: false, Term: 1}, nil)

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		followerRepl.sendInstallSnapshot(mockClient)
		s.wg.Wait()

		mockClient.AssertExpectations(t)
	})
}
