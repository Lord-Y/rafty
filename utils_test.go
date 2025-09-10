package rafty

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestUtils_getState(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	s.State = Down

	assert.Equal(Down, s.getState())
}

func TestUtils_votedFor(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	peerId := "0"
	term := uint64(1)
	s.setVotedFor(peerId, term)
	votedFor, votedForTerm := s.getVotedFor()
	assert.Equal(peerId, votedFor)
	assert.Equal(term, votedForTerm)
}

func TestUtils_getLeader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.Equal(leaderMap{}, s.getLeader())
	s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
	assert.Equal(s.id, s.getLeader().id)
	s.State = Leader
	assert.Equal(s.id, s.getLeader().id)
}

func TestUtils_getPeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	peers, total := s.getPeers()
	assert.NotNil(peers)
	assert.Equal(2, total)
}

func TestUtils_getAllPeers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	peers, total := s.getAllPeers()
	assert.NotNil(peers)
	assert.Equal(3, total)
}

func TestUtils_isRunning(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.Equal(false, s.IsRunning())
}

func TestUtils_parsePeers(t *testing.T) {
	assert := assert.New(t)

	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	peers := []InitialPeer{
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

	badPeers1 := []InitialPeer{
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

	badPeers2 := []InitialPeer{
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

	id := fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := Options{
		InitialPeers: peers,
		DataDir:      filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "basic_setup", id),
	}
	storeOptions := BoltOptions{
		DataDir: options.DataDir,
		Options: bbolt.DefaultOptions,
	}
	store, err := NewBoltStorage(storeOptions)
	assert.Nil(err)
	fsm := NewSnapshotState(store)
	s, _ := NewRafty(addr, id, options, store, fsm, nil)

	mergePeers := func(peers []InitialPeer) {
		for _, v := range peers {
			s.configuration.ServerMembers = append(s.configuration.ServerMembers, Peer{Address: v.Address})
		}
	}

	mergePeers(s.options.InitialPeers)
	err = s.parsePeers()
	assert.Nil(err)

	s.options.InitialPeers = badPeers1
	mergePeers(s.options.InitialPeers)
	err = s.parsePeers()
	assert.Error(err)

	s.configuration.ServerMembers = nil
	s.options.InitialPeers = badPeers2
	mergePeers(s.options.InitialPeers)
	err = s.parsePeers()
	assert.Error(err)
	assert.Nil(store.Close())
}

func TestUtils_switchStateAndLogState(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	tests := []struct {
		state                  State
		currentTerm            uint64
		niceMessage, isRunning bool
		expectedState          State
		expectedCurrentTerm    uint64
	}{
		{
			state:               Leader,
			currentTerm:         1,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       Leader,
			expectedCurrentTerm: 0,
		},
		{
			state:               Candidate,
			currentTerm:         2,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       Candidate,
			expectedCurrentTerm: 0,
		},
		{
			state:               Follower,
			currentTerm:         3,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       Follower,
			expectedCurrentTerm: 0,
		},
		// this block is duplicated on purpose
		{
			state:               Follower,
			currentTerm:         3,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       Follower,
			expectedCurrentTerm: 0,
		},
		{
			state:               ReadReplica,
			currentTerm:         3,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       ReadReplica,
			expectedCurrentTerm: 0,
		},
		{
			state:               Down,
			currentTerm:         3,
			niceMessage:         true,
			isRunning:           true,
			expectedState:       Down,
			expectedCurrentTerm: 0,
		},
		{
			state:               Follower,
			currentTerm:         3,
			niceMessage:         true,
			isRunning:           false,
			expectedState:       Down,
			expectedCurrentTerm: 0,
		},
	}

	s.State = Down
	for _, tc := range tests {
		s.isRunning.Store(tc.isRunning)
		s.switchState(tc.state, stepUp, tc.niceMessage, tc.currentTerm)
		assert.Equal(tc.expectedState, s.State)
		assert.Equal(tc.expectedCurrentTerm, s.currentTerm.Load())
	}
}

func TestUtils_saveLeaderInformations(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
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
			newLeader: leaderMap{address: s.options.InitialPeers[0].address.String()},
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

func TestUtils_min(t *testing.T) {
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

func TestUtils_max(t *testing.T) {
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

func TestUtils_backoff(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		wait        time.Duration
		failures    uint64
		maxFailures uint64
		result      time.Duration
	}{
		{
			wait:        time.Second,
			failures:    1,
			maxFailures: replicationMaxRetry,
			result:      time.Second,
		},
		{
			wait:        time.Second,
			failures:    10,
			maxFailures: replicationMaxRetry,
			result:      2 * time.Second,
		},
	}

	for _, tc := range tests {
		assert.Equal(tc.result, backoff(tc.wait, tc.failures, tc.maxFailures))
	}
}

func TestUtilsQuorum(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	assert.Equal(2, s.quorum())
}

func TestUtils_calculateMaxRangeLogIndex(t *testing.T) {
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

func TestUtils_getNetAddress(t *testing.T) {
	assert := assert.New(t)

	member := Peer{
		Address: "127.0.0.2:60000",
	}

	net := getNetAddress(member.Address)
	assert.NotNil(net.IP)
	assert.NotNil(net.Port)
}

func TestUtils_isPartOfTheCluster(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	member := Peer{
		Address: s.options.InitialPeers[2].Address,
	}
	assert.Equal(true, s.isPartOfTheCluster(member))
	assert.Equal(true, isPartOfTheCluster(s.configuration.ServerMembers, member))

	member.Address = "127.0.0.2:60000"
	member.ID = "fake"
	assert.Equal(false, s.isPartOfTheCluster(member))
	assert.Equal(false, isPartOfTheCluster(s.configuration.ServerMembers, member))
}

func TestUtils_waitForLeader(t *testing.T) {
	assert := assert.New(t)

	t.Run("error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.configuration.ServerMembers = nil
		assert.Equal(false, s.waitForLeader())
	})
}

func TestUtils_updateServerMembers(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	peers, total := s.getAllPeers()
	assert.NotNil(peers)
	assert.Equal(3, total)

	t.Run("waitToBePromoted", func(t *testing.T) {
		clone := slices.Clone(peers)
		clone[0].WaitToBePromoted = true
		s.updateServerMembers(clone)
		assert.Equal(true, s.waitToBePromoted.Load())
	})

	t.Run("decommissioning", func(t *testing.T) {
		clone := slices.Clone(peers)
		clone[0].WaitToBePromoted = false
		clone[0].Decommissioning = true
		s.updateServerMembers(clone)
		assert.Equal(false, s.waitToBePromoted.Load())
		assert.Equal(true, s.decommissioning.Load())
	})
}
