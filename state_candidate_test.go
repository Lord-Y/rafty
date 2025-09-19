package rafty

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateCandidate(t *testing.T) {
	assert := assert.New(t)

	t.Run("init", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})

		state := candidate{rafty: s}
		state.init()
		assert.Equal(Follower, s.getState())
	})

	t.Run("onTimeout_not_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		state := candidate{rafty: s}
		state.onTimeout()
		assert.Equal(Follower, s.getState())
	})

	t.Run("onTimeout_decommissioning", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		s.decommissioning.Store(true)
		state := candidate{rafty: s}
		state.onTimeout()
		assert.Equal(Follower, s.getState())
	})

	t.Run("onTimeout_prevote_enabled", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}
		state.onTimeout()
		assert.Equal(Candidate, s.getState())
	})

	t.Run("onTimeout_prevote_disabled", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.options.PrevoteDisabled = true
		s.State = Candidate
		state := candidate{rafty: s}
		state.onTimeout()
		assert.Equal(Candidate, s.getState())
	})

	t.Run("handlePreVoteResponse_not_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		state := candidate{rafty: s}
		state.handlePreVoteResponse(RPCResponse{})
	})

	t.Run("handlePreVoteResponse_response_current_term_greater", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate

		state := candidate{rafty: s}
		rpcResponse := RPCPreVoteResponse{
			CurrentTerm: 2,
		}
		state.handlePreVoteResponse(RPCResponse{
			Response: rpcResponse,
		})
	})

	t.Run("handlePreVoteResponse_response_current_term_greater_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate

		state := candidate{rafty: s}
		rpcResponse := RPCPreVoteResponse{
			CurrentTerm: 2,
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		state.handlePreVoteResponse(RPCResponse{
			Response: rpcResponse,
		})
	})

	t.Run("handlePreVoteResponse_start_election_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		state.startElection()
	})

	t.Run("handleVoteResponse_not_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		state := candidate{rafty: s}
		state.handleVoteResponse(RPCResponse{})
	})

	t.Run("handleVoteResponse_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		response := RPCResponse{
			TargetPeer: s.configuration.ServerMembers[0],
			Response: RPCVoteResponse{
				PeerID: s.configuration.ServerMembers[0].ID,
			},
			Error: ErrShutdown,
		}
		state.handleVoteResponse(response)
	})

	t.Run("handleVoteResponse_granted_false", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		response := RPCResponse{
			TargetPeer: s.configuration.ServerMembers[0],
			Response: RPCVoteResponse{
				PeerID: s.configuration.ServerMembers[0].ID,
			},
		}
		state.handleVoteResponse(response)
	})

	t.Run("handleVoteResponse_granted_true", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		response := RPCResponse{
			TargetPeer: s.configuration.ServerMembers[0],
			Response: RPCVoteResponse{
				PeerID:  s.configuration.ServerMembers[0].ID,
				Granted: true,
			},
		}
		state.handleVoteResponse(response)
	})

	t.Run("handleVoteResponse_step_down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		response := RPCResponse{
			TargetPeer: s.configuration.ServerMembers[0],
			Response: RPCVoteResponse{
				PeerID:        s.configuration.ServerMembers[0].ID,
				RequesterTerm: 1,
				CurrentTerm:   2,
			},
		}
		state.handleVoteResponse(response)
		assert.Equal(Follower, s.getState())
	})

	t.Run("handleVoteResponse_step_down_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.State = Candidate
		state := candidate{rafty: s}

		response := RPCResponse{
			TargetPeer: s.configuration.ServerMembers[0],
			Response: RPCVoteResponse{
				PeerID:        s.configuration.ServerMembers[0].ID,
				RequesterTerm: 1,
				CurrentTerm:   2,
			},
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		state.handleVoteResponse(response)
		assert.Equal(Follower, s.getState())
	})

	t.Run("isSingleServerCluster_leader", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.options.IsSingleServerCluster = true
		s.State = Candidate
		state := candidate{rafty: s}
		state.isSingleServerCluster()
		assert.Equal(Leader, s.getState())
	})

	t.Run("isSingleServerCluster_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.options.IsSingleServerCluster = true
		s.State = Candidate
		state := candidate{rafty: s}

		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		state.isSingleServerCluster()
	})
}
