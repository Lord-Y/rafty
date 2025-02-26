package rafty

import (
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestGetState(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.State = Down

	assert.Equal(Down, s.getState())
}

func TestCurrentTerm(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setCurrentTerm(uint64(1))
	assert.Equal(uint64(1), s.getCurrentTerm())
	s.incrementCurrentTerm()
	assert.Equal(uint64(2), s.getCurrentTerm())
}

func TestGetCommitIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(uint64(0), s.getCommitIndex())
}

func TestGetMyAddress(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	addr, id := s.getMyAddress()
	assert.Equal(s.Address.String(), addr)
	assert.Equal(s.ID, id)
}

func TestNextIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setNextAndMatchIndex(s.ID, 0, 0)
	for _, peer := range s.Peers {
		s.setNextAndMatchIndex(peer.id, 0, 0)
	}
	assert.Equal(uint64(0), s.getNextIndex(s.ID))
	peerId := s.Peers[0].id
	assert.Equal(uint64(0), s.getNextIndex(peerId))
	s.setNextIndex(peerId, uint64(1))
	assert.Equal(uint64(1), s.getNextIndex(peerId))
	assert.Equal(uint64(0), s.getNextIndex("plop"))
}

func TestNextAndMatchIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setNextAndMatchIndex(s.ID, 0, 0)
	for _, peer := range s.Peers {
		s.setNextAndMatchIndex(peer.id, 0, 0)
	}
	n, m := s.getNextAndMatchIndex(s.ID)
	assert.Equal(uint64(0), n)
	assert.Equal(uint64(0), m)
}

func TestGetLastLogIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(uint64(0), s.getLastLogIndex())
}

func TestGetLastLogTerm(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.log = append(s.log, &raftypb.LogEntry{})
	assert.Equal(uint64(0), s.getLastLogTerm(s.log[s.lastLogIndex].Term))
	s.log = append(s.log, &raftypb.LogEntry{Term: 1})
	assert.Equal(uint64(0), s.getLastLogTerm(s.log[s.lastLogIndex].Term))
}

func TestGetX(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.log = append(s.log, &raftypb.LogEntry{})
	assert.Equal(uint64(0), s.getX(s.log[s.lastLogIndex].Term))
	s.log = append(s.log, &raftypb.LogEntry{Term: 1})
	assert.Equal(uint64(0), s.getX(s.log[s.lastLogIndex].Term))
}

func TestVotedFor(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	peer := s.Peers[0]
	term := uint64(1)
	s.setVotedFor(peer.id, term)
	votedFor, votedForTerm := s.getVotedFor()
	assert.Equal(peer.id, votedFor)
	assert.Equal(term, votedForTerm)
}

func TestPrecandidate(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(s.configuration.preCandidatePeers, s.getPrecandidate())

	peer := peer{
		Address: "127.0.0.4:50054",
		ID:      "d",
	}
	s.appendPrecandidate(peer)
	assert.Equal(s.configuration.preCandidatePeers, s.getPrecandidate())
}

func TestGetMinimumClusterSize(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.MinimumClusterSize = 3
	assert.Equal(uint64(3), s.getMinimumClusterSize())

	peer := peer{
		Address: "127.0.0.4:50054",
		ID:      "d",
	}
	s.appendPrecandidate(peer)
	s.MinimumClusterSize = 4
	assert.Equal(uint64(4), s.getMinimumClusterSize())
}

func TestGetTotalLogs(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(0, s.getTotalLogs())
	s.log = append(s.log, &raftypb.LogEntry{})
	assert.Equal(1, s.getTotalLogs())
}

func TestSetLeaderLastContactDate(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setLeaderLastContactDate()
	assert.NotNil(s.LeaderLastContactDate)
}

func TestGetLeader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	s.setLeader(leaderMap{address: s.Address.String(), id: s.ID})
	assert.Equal(s.ID, s.getLeader().id)
	s.State = Leader
	assert.Equal(s.ID, s.getLeader().id)
}

func TestIncrementLeaderCommitIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(uint64(1), s.incrementLeaderCommitIndex())
}

func TestIncrementLastApplied(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	assert.Equal(uint64(1), s.incrementLastApplied())
}
