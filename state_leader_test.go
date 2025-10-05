package rafty

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestStateLeader(t *testing.T) {
	assert := assert.New(t)

	t.Run("onTimeout_not_leader", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		state := leader{rafty: s}
		state.onTimeout()
	})

	t.Run("onTimeout_decommissioning", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		s.decommissioning.Store(true)
		state := leader{rafty: s}
		state.onTimeout()
		assert.Equal(Follower, s.getState())
	})

	t.Run("leadershipTransfer_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}

		state.leadershipTransferDuration = state.rafty.heartbeatTimeout()
		state.leadershipTransferTimer = time.NewTicker(state.leadershipTransferDuration)
		state.leadershipTransferChan = make(chan RPCResponse, 1)

		response := RPCResponse{
			TargetPeer: Peer{ID: "xxxx", Address: "127.0.0.1:60000", address: getNetAddress("127.0.0.1:60000")},
			Response:   RPCTimeoutNowResponse{},
			Error:      ErrShutdown,
		}
		s.leadershipTransferInProgress.Store(true)
		go state.leadershipTransferLoop()

		state.leadershipTransferChan <- response
		s.wg.Add(1)
		go func() {
			s.wg.Done()
			time.Sleep(time.Second)
			close(state.leadershipTransferChan)
			state.leadershipTransferChanClosed.Store(true)
		}()

		time.Sleep(time.Second)
		s.wg.Wait()
	})

	t.Run("leadershipTransfer_success_false", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}

		state.leadershipTransferDuration = state.rafty.heartbeatTimeout()
		state.leadershipTransferTimer = time.NewTicker(state.leadershipTransferDuration)
		state.leadershipTransferChan = make(chan RPCResponse, 1)

		response := RPCResponse{
			TargetPeer: Peer{ID: "xxxx", Address: "127.0.0.1:60000", address: getNetAddress("127.0.0.1:60000")},
			Response:   RPCTimeoutNowResponse{Success: false},
		}
		s.leadershipTransferInProgress.Store(true)
		go state.leadershipTransferLoop()

		state.leadershipTransferChan <- response
		s.wg.Add(1)
		go func() {
			s.wg.Done()
			time.Sleep(time.Second)
			close(state.leadershipTransferChan)
			state.leadershipTransferChanClosed.Store(true)
		}()

		time.Sleep(time.Second)
		s.wg.Wait()
	})

	t.Run("leadershipTransfer_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}

		state.leadershipTransferDuration = state.rafty.heartbeatTimeout()
		state.leadershipTransferTimer = time.NewTicker(state.leadershipTransferDuration)
		state.leadershipTransferChan = make(chan RPCResponse, 1)

		s.leadershipTransferInProgress.Store(true)
		go state.leadershipTransferLoop()
		time.Sleep(time.Second)
	})

	t.Run("release_single_server_cluster", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}

		state.leaseDuration = state.rafty.heartbeatTimeout()
		state.leaseTimer = time.NewTicker(state.leaseDuration * 3)
		state.leadershipTransferDuration = state.rafty.heartbeatTimeout()
		state.leadershipTransferTimer = time.NewTicker(state.leadershipTransferDuration)
		s.options.IsSingleServerCluster = true

		state.release()
	})

	t.Run("replicateLog_membership_in_progress_shutdown", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()
		s.membershipChangeInProgress.Store(true)

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: "127.0.0.1",
					ID:      "6000",
				},
				action: Add,
			},
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-responseChan
		}()

		state.replicateLog(request)
	})

	t.Run("replicateLog_membership_in_progress_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		s.membershipChangeInProgress.Store(true)

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: "127.0.0.1",
					ID:      "6000",
				},
				action: Add,
			},
		}

		state.replicateLog(request)
		data := <-responseChan
		assert.ErrorIs(data.Error, ErrMembershipChangeInProgress)
	})

	t.Run("replicateLog_membership_in_progress_timeout_chan", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		s.membershipChangeInProgress.Store(true)

		request := replicateLogConfig{
			logType: LogConfiguration,
			source:  "forward",
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: "127.0.0.1",
					ID:      "6000",
				},
				action: Add,
			},
		}

		state.replicateLog(request)
	})

	t.Run("replicateLog_membership_demote_shutdown", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Demote,
			},
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-responseChan
		}()

		state.replicateLog(request)
	})

	t.Run("replicateLog_membership_demote_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Demote,
			},
		}

		s.configuration.ServerMembers[0].Decommissioning = true
		state.replicateLog(request)
		data := <-responseChan
		assert.ErrorIs(data.Error, ErrMembershipChangeNodeDemotionForbidden)
	})

	t.Run("replicateLog_membership_demote_timeout_chan", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)

		request := replicateLogConfig{
			logType: LogConfiguration,
			source:  "forward",
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Demote,
			},
		}

		s.configuration.ServerMembers[0].Decommissioning = true
		state.replicateLog(request)
	})

	t.Run("replicateLog_membership_remove_shutdown", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Remove,
			},
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-responseChan
		}()

		state.replicateLog(request)
	})

	t.Run("replicateLog_membership_remove_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)

		responseChan := make(chan RPCResponse, 1)
		request := replicateLogConfig{
			logType:    LogConfiguration,
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Remove,
			},
		}

		state.replicateLog(request)
		data := <-responseChan
		assert.ErrorIs(data.Error, ErrMembershipChangeNodeNotDemoted)
	})

	t.Run("replicateLog_membership_demote_timeout_chan", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)

		request := replicateLogConfig{
			logType: LogConfiguration,
			source:  "forward",
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.configuration.ServerMembers[1].Address,
					ID:      s.configuration.ServerMembers[1].ID,
				},
				action: Demote,
			},
		}

		state.replicateLog(request)
	})

	t.Run("replicateLog_commit_loop_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)

		s.murw.Lock()
		state.commitIndexWatcher[1] = &indexWatcher{
			totalFollowers: 1,
			quorum:         1,
			term:           1,
			logType:        LogConfiguration,
			uuid:           uuid.NewString(),
			logs: []*LogEntry{
				{
					Term:  1,
					Index: 1,
				},
			},
			source: "forward",
			client: true,
		}
		s.murw.Unlock()

		go state.commitLoop()
		state.commitChan <- commitChanConfig{id: s.configuration.ServerMembers[1].ID, matchIndex: 1}
		time.Sleep(100 * time.Millisecond)
		s.State = Follower
		s.wg.Wait()
	})

	t.Run("replicateLog_commit_loop_shutdown_on_remove", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		s.options.ShutdownOnRemove = true
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)
		s.grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))

		s.murw.Lock()
		responseChan := make(chan RPCResponse, 1)
		state.commitIndexWatcher[1] = &indexWatcher{
			totalFollowers: 1,
			quorum:         1,
			term:           1,
			logType:        LogConfiguration,
			uuid:           uuid.NewString(),
			logs: []*LogEntry{
				{
					Term:  1,
					Index: 1,
				},
			},
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.Address.String(),
					ID:      s.id,
				},
				action: Remove,
			},
		}
		s.murw.Unlock()

		go state.commitLoop()
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-responseChan
		}()
		state.commitChan <- commitChanConfig{id: s.configuration.ServerMembers[1].ID, matchIndex: 1}
	})

	t.Run("replicateLog_commit_loop_demote", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		state := leader{rafty: s}
		s.options.ShutdownOnRemove = true
		state.commitChan = make(chan commitChanConfig)
		state.commitIndexWatcher = make(map[uint64]*indexWatcher)
		s.grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))

		s.murw.Lock()
		responseChan := make(chan RPCResponse, 1)
		state.commitIndexWatcher[1] = &indexWatcher{
			totalFollowers: 1,
			quorum:         1,
			term:           1,
			logType:        LogConfiguration,
			uuid:           uuid.NewString(),
			logs: []*LogEntry{
				{
					Term:  1,
					Index: 1,
				},
			},
			source:     "forward",
			client:     true,
			clientChan: responseChan,
			membershipChange: struct {
				action MembershipChange
				member Peer
			}{
				member: Peer{
					Address: s.Address.String(),
					ID:      s.id,
				},
				action: Demote,
			},
		}
		s.murw.Unlock()

		go state.commitLoop()
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-responseChan
		}()
		state.commitChan <- commitChanConfig{id: s.configuration.ServerMembers[1].ID, matchIndex: 1}
		s.wg.Wait()
	})
}
