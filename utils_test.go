package rafty

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// basicNodeSetup is only a helper for other unit testing.
// It MUST NOT be used to start a cluster
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
		},
		{
			Address: "127.0.0.3:50053",
		},
	}

	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	options := Options{
		Peers: peers,
	}
	s := NewRafty(addr, id, options)

	for _, v := range s.options.Peers {
		s.configuration.ServerMembers = append(s.configuration.ServerMembers, peer{Address: v.Address})
	}
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

	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	options := Options{
		Peers: peers,
	}
	s := NewRafty(addr, id, options)

	mergePeers := func(peers []Peer) {
		for _, v := range peers {
			s.configuration.ServerMembers = append(s.configuration.ServerMembers, peer{Address: v.Address})
		}
	}

	mergePeers(s.options.Peers)

	err := s.parsePeers()
	assert.Nil(err)

	s.options.Peers = badPeers1
	mergePeers(s.options.Peers)
	err = s.parsePeers()
	assert.Error(err)

	s.options.Peers = badPeers2
	mergePeers(s.options.Peers)
	err = s.parsePeers()
	assert.Error(err)
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
		s.switchState(tc.state, stepUp, tc.niceMessage, tc.currentTerm)
		assert.Equal(tc.expectedState, s.State)
		assert.Equal(tc.expectedCurrentTerm, s.currentTerm.Load())
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
			newLeader:           leaderMap{address: s.Address.String(), id: s.id},
			currentTerm:         1,
			switchState:         true,
			niceMessage:         true,
			expectedState:       Leader,
			expectedCurrentTerm: 1,
		},
		// this block is duplicated on purpose
		{
			state:               Leader,
			newLeader:           leaderMap{address: s.Address.String(), id: s.id},
			currentTerm:         1,
			switchState:         true,
			niceMessage:         true,
			expectedState:       Leader,
			expectedCurrentTerm: 1,
		},
		{
			newLeader: leaderMap{address: s.options.Peers[0].address.String()},
		},
		{
			newLeader: leaderMap{address: s.Address.String(), id: s.id},
		},
	}

	s.State = Candidate
	for _, tc := range tests {
		s.currentTerm.Store(tc.currentTerm)
		if tc.switchState {
			s.switchState(tc.state, stepUp, tc.niceMessage, tc.currentTerm)
			assert.Equal(tc.expectedState, s.State)
			assert.Equal(tc.expectedCurrentTerm, s.currentTerm.Load())
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

func TestCalculateMaxRangeLogIndex(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		totalLogs        uint
		maxAppendEntries uint
		initialIndex     uint
		limit            uint
		snapshot         bool
	}{
		{
			totalLogs:        50,
			maxAppendEntries: 64,
			initialIndex:     0,
			limit:            50,
			snapshot:         false,
		},
		{
			totalLogs:        100,
			maxAppendEntries: 64,
			initialIndex:     0,
			limit:            64,
			snapshot:         true,
		},
		{
			totalLogs:        100,
			maxAppendEntries: 1,
			initialIndex:     4,
			limit:            5,
			snapshot:         false,
		},
	}

	for _, tc := range tests {
		limit, snapshot := calculateMaxRangeLogIndex(tc.totalLogs, tc.maxAppendEntries, tc.initialIndex)
		assert.Equal(tc.limit, limit)
		assert.Equal(tc.snapshot, snapshot)
	}
}
