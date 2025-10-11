package rafty

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
)

func TestStateLoop_runAsFollower(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = Follower
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("installSnapshot", func(t *testing.T) {
		s.currentTerm.Store(2)
		go s.runAsFollower()
		responseChan := make(chan RPCResponse, 1)
		s.rpcInstallSnapshotRequestChan <- RPCRequest{
			RPCType:      InstallSnapshotRequest,
			Request:      &raftypb.InstallSnapshotRequest{CurrentTerm: 1},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrTermTooOld, data.Error)
	})
}

func TestStateLoop_runAsCandidate(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = Candidate
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("installSnapshot", func(t *testing.T) {
		s.currentTerm.Store(2)
		go s.runAsCandidate()
		responseChan := make(chan RPCResponse, 1)
		s.rpcInstallSnapshotRequestChan <- RPCRequest{
			RPCType:      InstallSnapshotRequest,
			Request:      &raftypb.InstallSnapshotRequest{CurrentTerm: 1},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrTermTooOld, data.Error)
	})
}

func TestStateLoop_runAsLeader(t *testing.T) {
	assert := assert.New(t)

	t.Run("handleSendVoteRequest", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(2)
		id := 1
		go s.runAsLeader()
		responseChan := make(chan RPCResponse, 1)
		s.rpcVoteRequestChan <- RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      s.configuration.ServerMembers[id].ID,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		data := <-responseChan
		assert.Equal(nil, data.Error)
	})

	t.Run("handleSendAppendEntriesRequest", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(2)
		id := 1
		go s.runAsLeader()
		responseChan := make(chan RPCResponse, 1)
		s.rpcAppendEntriesReplicationRequestChan <- RPCRequest{
			RPCType: AppendEntriesReplicationRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      s.configuration.ServerMembers[id].ID,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          1,
			},
			ResponseChan: responseChan,
		}

		data := <-responseChan
		assert.Equal(nil, data.Error)
	})

	t.Run("installSnapshot", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(2)
		go s.runAsLeader()
		responseChan := make(chan RPCResponse, 1)
		s.rpcInstallSnapshotRequestChan <- RPCRequest{
			RPCType:      InstallSnapshotRequest,
			Request:      &raftypb.InstallSnapshotRequest{CurrentTerm: 1},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrTermTooOld, data.Error)
	})
}

func TestStateLoop_snapshotLoop(t *testing.T) {
	assert := assert.New(t)

	t.Run("no_snapshot", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(1)
		s.options.SnapshotInterval = 2 * time.Second
		go s.snapshotLoop()
		time.Sleep(3 * time.Second)
		s.stopCtx()
	})

	t.Run("snapshot_taken", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(1)
		s.options.SnapshotInterval = 2 * time.Second

		for index := range 100 {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			s.storeLogs(logs)
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}
		go s.snapshotLoop()
		time.Sleep(3 * time.Second)
		s.stopCtx()
	})

	t.Run("snapshot_fail", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()

		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.isRunning.Store(true)
		s.State = Leader
		s.timer = time.NewTicker(s.randomElectionTimeout())
		s.currentTerm.Store(1)
		s.options.SnapshotInterval = 2 * time.Second

		snapshotTestHook = func() error { return errors.New("test error") }
		for index := range 100 {
			entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			s.storeLogs(logs)
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}
		go s.snapshotLoop()
		time.Sleep(3 * time.Second)
		s.stopCtx()
	})
}
