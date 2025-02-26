package rafty

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func basicNodeSetup() *Rafty {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	peers := []Peer{
		{
			Address: "127.0.0.1",
		},
		{
			Address: "127.0.0.2",
			id:      "b",
		},
		{
			Address: "127.0.0.3:50053",
			id:      "c",
		},
	}

	s := NewServer(addr)
	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	s.ID = id
	s.Peers = peers

	for _, v := range s.Peers {
		s.configuration.ServerMembers = append(s.configuration.ServerMembers, peer{ID: v.id, Address: v.Address})
	}
	s.configuration.preCandidatePeers = s.configuration.ServerMembers
	return s
}

func TestParsePeers(t *testing.T) {
	assert := assert.New(t)

	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	peers := []Peer{
		{
			Address: "127.0.0.1",
		},
		{
			Address: "127.0.0.2",
		},
		{
			Address: "127.0.0.3:50053",
		},
	}

	badPeers1 := []Peer{
		{
			Address: "127.0.0.1:50051",
		},
		{
			Address: "127.0.0.2:50052",
		},
		{
			Address: "127.0.0.3:aaaa",
		},
	}

	badPeers2 := []Peer{
		{
			Address: "127.0.0.1:50051",
		},
		{
			Address: "127.0.0.2:50052",
		},
		{
			Address: "[127.0.0.2:b",
		},
		{
			Address: "[foo]:[bar]baz",
		},
		{
			Address: "127.0.0.4::aaaa",
		},
	}

	s := NewServer(addr)
	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	s.ID = id
	s.Peers = peers

	mergePeers := func(peers []Peer) {
		for _, v := range peers {
			s.configuration.ServerMembers = append(s.configuration.ServerMembers, peer{ID: v.id, Address: v.Address})
		}
		s.configuration.preCandidatePeers = s.configuration.ServerMembers
	}

	mergePeers(s.Peers)

	err := s.parsePeers()
	assert.Nil(err)

	s.Peers = badPeers1
	mergePeers(s.Peers)
	err = s.parsePeers()
	assert.Error(err)

	s.Peers = badPeers2
	mergePeers(s.Peers)
	err = s.parsePeers()
	assert.Error(err)
}

func TestGetPeerSliceIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	peer := s.configuration.ServerMembers[0].address.String()
	index := s.getPeerSliceIndex(peer)
	assert.Equal(0, index)

	fake := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}

	index = s.getPeerSliceIndex(fake.String())
	assert.Equal(-1, index)
}

func TestCheckIfPeerInSliceIndex(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	peer := s.configuration.ServerMembers[0].address.String()
	index := s.checkIfPeerInSliceIndex(false, peer)
	assert.Equal(true, index)

	index = s.checkIfPeerInSliceIndex(true, peer)
	assert.Equal(true, index)

	fake := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}

	index = s.checkIfPeerInSliceIndex(false, fake.String())
	assert.Equal(false, index)
}

func TestSwitchStateAndLogState(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	tests := []struct {
		state               State
		currentTerm         uint64
		niceMessage         bool
		expectedState       State
		expectedCurrentTerm uint64
	}{
		{
			state:               Leader,
			currentTerm:         1,
			niceMessage:         true,
			expectedState:       Leader,
			expectedCurrentTerm: 0,
		},
		{
			state:               Candidate,
			currentTerm:         2,
			niceMessage:         true,
			expectedState:       Candidate,
			expectedCurrentTerm: 0,
		},
		{
			state:               Follower,
			currentTerm:         3,
			niceMessage:         true,
			expectedState:       Follower,
			expectedCurrentTerm: 0,
		},
		// this block is duplicated on purpose
		{
			state:               Follower,
			currentTerm:         3,
			niceMessage:         true,
			expectedState:       Follower,
			expectedCurrentTerm: 0,
		},
		{
			state:               ReadOnly,
			currentTerm:         3,
			niceMessage:         true,
			expectedState:       ReadOnly,
			expectedCurrentTerm: 0,
		},
		{
			state:               Down,
			currentTerm:         3,
			niceMessage:         true,
			expectedState:       Down,
			expectedCurrentTerm: 0,
		},
	}

	s.State = Down
	for _, tc := range tests {
		s.logState(tc.expectedState, tc.niceMessage, tc.currentTerm)
		s.switchState(tc.state, tc.niceMessage, tc.currentTerm)
		assert.Equal(tc.expectedState, s.State)
		assert.Equal(tc.expectedCurrentTerm, s.CurrentTerm)
		s.logState(tc.expectedState, tc.niceMessage, tc.currentTerm)
	}
}

func TestSaveLeaderInformations(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	tests := []struct {
		state               State
		newLeader           leaderMap
		expectedLeader      leaderMap
		currentTerm         uint64
		switchState         bool
		niceMessage         bool
		expectedState       State
		expectedCurrentTerm uint64
	}{
		{
			state:               Leader,
			newLeader:           leaderMap{address: s.Address.String(), id: s.ID},
			currentTerm:         1,
			switchState:         true,
			niceMessage:         true,
			expectedState:       Leader,
			expectedCurrentTerm: 1,
		},
		// this block is duplicated on purpose
		{
			state:               Leader,
			newLeader:           leaderMap{address: s.Address.String(), id: s.ID},
			currentTerm:         1,
			switchState:         true,
			niceMessage:         true,
			expectedState:       Leader,
			expectedCurrentTerm: 1,
		},
		{
			newLeader: leaderMap{address: s.Peers[0].address.String(), id: s.Peers[0].id},
		},
		{
			newLeader: leaderMap{address: s.Address.String(), id: s.ID},
		},
	}

	s.State = Candidate
	for _, tc := range tests {
		s.setCurrentTerm(tc.currentTerm)
		if tc.switchState {
			s.switchState(tc.state, tc.niceMessage, tc.currentTerm)
			assert.Equal(tc.expectedState, s.State)
			assert.Equal(tc.expectedCurrentTerm, s.CurrentTerm)
			s.setLeader(tc.newLeader)
		} else {
			s.State = Candidate
			s.setLeader(leaderMap{})
			s.setLeader(tc.newLeader)
			s.setLeader(tc.newLeader)
		}
	}
}

func TestMin(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		a      uint64
		b      uint64
		result uint64
	}{
		{
			a:      50,
			b:      50,
			result: 50,
		},
		{
			a:      50,
			b:      70,
			result: 50,
		},
		{
			a:      100,
			b:      70,
			result: 70,
		},
	}

	for _, tc := range tests {
		assert.Equal(tc.result, min(tc.a, tc.b))
	}
}

func TestMax(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		a      uint64
		b      uint64
		result uint64
	}{
		{
			a:      50,
			b:      50,
			result: 50,
		},
		{
			a:      100,
			b:      70,
			result: 100,
		},
	}

	for _, tc := range tests {
		assert.Equal(tc.result, max(tc.a, tc.b))
	}
}

func TestGetPeerClient(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	server := s.configuration.ServerMembers[0]
	index := s.getPeerClient(server.ID)
	assert.Equal(server, index)

	index = s.getPeerClient("xxxxx")
	assert.Equal(peer{}, index)
}
