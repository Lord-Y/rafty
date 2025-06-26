package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateCandidate(t *testing.T) {
	assert := assert.New(t)

	t.Run("init", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.isRunning.Store(true)
		s.State = Candidate
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})

		state := candidate{rafty: s}
		state.init()
		assert.Equal(Follower, s.getState())
	})

	t.Run("ontimeout", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.isRunning.Store(true)
		s.State = Follower
		state := candidate{rafty: s}
		state.onTimeout()
		assert.Equal(Follower, s.getState())
	})

	t.Run("handlePreVoteResponse_not_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		state := candidate{rafty: s}
		state.handlePreVoteResponse(RPCResponse{})
	})

	t.Run("handlePreVoteResponse_response_current_term_greater", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

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

	t.Run("handleVoteResponse_not_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		state := candidate{rafty: s}
		state.handleVoteResponse(RPCResponse{})
	})

	t.Run("handleVoteResponse_error", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

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
		err := s.parsePeers()
		assert.Nil(err)

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
		err := s.parsePeers()
		assert.Nil(err)

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
		err := s.parsePeers()
		assert.Nil(err)

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
}
