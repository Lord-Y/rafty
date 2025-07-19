package rafty

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetState(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.State = Down

	assert.Equal(Down, s.getState())
}

func TestVotedFor(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	peerId := "0"
	term := uint64(1)
	s.setVotedFor(peerId, term)
	votedFor, votedForTerm := s.getVotedFor()
	assert.Equal(peerId, votedFor)
	assert.Equal(term, votedForTerm)
}

func TestGetLeader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
	assert.Equal(s.id, s.getLeader().id)
	s.State = Leader
	assert.Equal(s.id, s.getLeader().id)
}

func TestGetPeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	peers, total := s.getPeers()
	assert.NotNil(peers)
	assert.Equal(2, total)
}

func TestGetAllPeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	peers, total := s.getAllPeers()
	assert.NotNil(peers)
	assert.Equal(3, total)
}

func TestIsRunning(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(false, s.IsRunning())
}
