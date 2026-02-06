package rafty

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestLogReplication_SendCatchupAppendEntries(t *testing.T) {
	assert := assert.New(t)

	t.Run("total_zero", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		entries := makeProtobufLogEntry(entry)

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())
		currentTerm := s.currentTerm.Add(1)

		oldRequest := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
		s.wg.Wait()
	})

	t.Run("timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		entries := makeProtobufLogEntry(entry)

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())
		currentTerm := s.currentTerm.Add(1)

		oldRequest := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
		s.wg.Wait()
	})

	t.Run("sendSnapshot", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()
		s.options.MaxAppendEntries = 64
		s.currentTerm.Store(1)

		max := 100
		var entries []*raftypb.LogEntry
		for index := range max {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			assert.Nil(s.storeLogs(logs))
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
			entries = append(entries, makeProtobufLogEntry(entry)...)
		}

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())
		currentTerm := s.currentTerm.Add(1)

		oldRequest := &onAppendEntriesRequest{
			term:                       currentTerm,
			prevLogIndex:               s.lastLogIndex.Load(),
			prevLogTerm:                s.lastLogTerm.Load(),
			totalLogs:                  s.lastLogIndex.Load(),
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
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendCatchupAppendEntries(client, oldRequest, oldResponse)
		s.wg.Wait()
	})
}

func TestLogReplication_catchupNewMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("shutdown", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		member := Peer{ID: "newbie", Address: "127.0.0.1:60000"}

		currentTerm := s.currentTerm.Load()
		peers, _ := s.getAllPeers()

		peers = append(peers, member)
		action := Add
		nextConfig, _ := s.nextConfiguration(action, peers, member)
		encodedPeers := EncodePeers(nextConfig)

		entry := makeNewLogEntry(currentTerm, LogConfiguration, encodedPeers)
		assert.Nil(s.storeLogs([]*LogEntry{entry}))
		previousLogIndex, previousTerm := s.getPreviousLogIndexAndTerm()

		request := &onAppendEntriesRequest{
			term:               currentTerm,
			prevLogIndex:       previousLogIndex,
			prevLogTerm:        previousTerm,
			commitIndex:        s.commitIndex.Load(),
			catchup:            true,
			rpcTimeout:         time.Second,
			membershipChangeID: member.ID,
		}

		follower := &followerReplication{
			Peer:             member,
			rafty:            s,
			newEntryChan:     make(chan replicationData),
			notifyLeaderChan: state.commitChan,
		}

		buildResponse := &raftypb.AppendEntryResponse{
			LogNotFound: true,
		}

		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		s.stopCtx()

		assert.ErrorIs(follower.catchupNewMember(member, request, buildResponse), ErrShutdown)
	})
}

func TestLogReplication_sendInstallSnapshot(t *testing.T) {
	t.Run("sendSnapshotInProgress", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan replicationData),
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
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		followers, _ := s.getPeers()

		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendInstallSnapshot(client)
		s.wg.Wait()
	})

	t.Run("errInstallSnapshot", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()
		s.currentTerm.Store(1)

		max := 100
		for index := range max {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			assert.Nil(s.storeLogs(logs))
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		id := 0
		client := s.connectionManager.getClient(s.configuration.ServerMembers[id].address.String())

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendInstallSnapshot(client)
		s.wg.Wait()
	})

	t.Run("response_success_true", func(t *testing.T) {
		assert := assert.New(t)
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()
		s.currentTerm.Store(1)

		max := 100
		for index := range max {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			assert.Nil(s.storeLogs(logs))
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
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
			newEntryChan: make(chan replicationData),
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
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.isRunning.Store(true)
		s.State = Leader
		s.options.MaxAppendEntries = 64
		followers, _ := s.getPeers()
		s.currentTerm.Store(1)

		max := 100
		for index := range max {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			assert.Nil(s.storeLogs(logs))
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		id := 0
		mockClient := new(MockRaftyClientTestify)
		mockClient.On("SendInstallSnapshotRequest", mock.Anything, mock.Anything, mock.Anything).
			Return(&raftypb.InstallSnapshotResponse{Success: false, Term: 2}, nil)

		followerRepl := &followerReplication{
			Peer:         followers[id],
			rafty:        s,
			newEntryChan: make(chan replicationData),
		}
		followerRepl.sendInstallSnapshot(mockClient)
		s.wg.Wait()

		mockClient.AssertExpectations(t)
	})
}
