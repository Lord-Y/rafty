package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateCandidateInit_follower(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.State = Candidate
	s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})

	state := candidate{rafty: s}
	state.init()
	assert.Equal(Follower, s.getState())
}

func TestStateCandidateOntimeout_follower(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.State = Follower
	state := candidate{rafty: s}
	state.onTimeout()
	assert.Equal(Follower, s.getState())
}

func TestStateCandidate_handlePreVoteResponse_not_candidate(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.State = Leader
	s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

	state := candidate{rafty: s}
	state.handlePreVoteResponse(RPCResponse{})
}

func TestStateCandidate_handleVoteResponse_not_candidate(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.State = Leader
	s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

	state := candidate{rafty: s}
	state.handleVoteResponse(RPCResponse{})
}

func TestStateCandidate_handleVoteResponse_error(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

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
}

func TestStateCandidate_handleVoteResponse_step_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

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
}
