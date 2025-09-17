package rafty

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateLeader(t *testing.T) {
	assert := assert.New(t)

	t.Run("onTimeout_not_leader", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
}
