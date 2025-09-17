package rafty

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMembershipString(t *testing.T) {
	assert := assert.New(t)

	tests := []MembershipChange{
		Add,
		Promote,
		Demote,
		Remove,
		ForceRemove,
	}
	results := []string{
		"add",
		"promote",
		"demote",
		"remove",
		"forceRemove",
	}

	for k, v := range tests {
		assert.Equal(v.String() == results[k], true)
	}
}

func TestMembership_nextConfiguration(t *testing.T) {
	assert := assert.New(t)

	t.Run("add", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              Peer
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode", ReadReplica: true},
				expWaitToBePromoted: true,
				expReadReplica:      true,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("remove", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              Peer
			action              MembershipChange
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode"},
				action:              Remove,
				expWaitToBePromoted: false,
				expContained:        false,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode", ReadReplica: true},
				action:              ForceRemove,
				expWaitToBePromoted: false,
				expContained:        false,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(tc.action, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("promote", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              Peer
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode", ReadReplica: true},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expReadReplica:      true,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("demoted", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              Peer
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              Peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              Peer{ID: "newnode", ReadReplica: true},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expReadReplica:      true,
				expContained:        true,
				expVerify:           true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Demote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("all", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              Peer
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    Peer{ID: "newnode"},
				expVerify: true,
			},
			{
				member:    Peer{ID: "newnode", ReadReplica: true},
				expVerify: true,
			},
		}

		for i, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)

			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Demote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Remove, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("breaking", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			action              MembershipChange
			member              Peer
			memberid            int
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				action:       Demote,
				memberid:     0,
				expContained: true,
				expVerify:    true,
			},
			{
				action:    Demote,
				memberid:  1,
				expVerify: false,
				expError:  ErrMembershipChangeNodeDemotionForbidden,
			},
		}

		for _, tc := range tests {
			peers, _ := s.getAllPeers()
			tc.member = peers[tc.memberid]
			next, err := state.nextConfiguration(tc.action, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
				assert.Equal(tc.expError, err)
				assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
				assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
				assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
				assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
				assert.Equal(tc.expVerify, state.verifyConfiguration(next))
			}
		}
	})

	t.Run("leaveOnTerminate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		peers, _ := s.getAllPeers()
		tests := []struct {
			member              Peer
			expReadReplica      bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    Peer{ID: "newnode"},
				expVerify: true,
			},
			{
				member:    Peer{ID: "newnode", ReadReplica: true},
				expVerify: true,
			},
		}

		for i, tc := range tests {
			tc.member.ID = fmt.Sprintf("%s_%d", tc.member.ID, i)
			tc.member.Address = fmt.Sprintf("127.0.0.1:%d", 60000+i)
			next, err := state.nextConfiguration(Add, peers, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(Promote, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}
			assert.Equal(tc.expError, err)
			next, err = state.nextConfiguration(LeaveOnTerminate, next, tc.member)
			if err == nil {
				s.updateServerMembers(next)
			}

			assert.Equal(tc.expError, err)
			assert.Equal(tc.expWaitToBePromoted, next[len(next)-1].WaitToBePromoted)
			assert.Equal(tc.expDecommissioning, next[len(next)-1].Decommissioning)
			assert.Equal(tc.expReadReplica, next[len(next)-1].ReadReplica)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})
}

func TestMembership_changeRequest(t *testing.T) {
	assert := assert.New(t)

	member := Peer{
		ID:      "newbie",
		Address: "127.0.0.1:60000",
	}
	member.address = getNetAddress(member.Address)
	responseChan := make(chan RPCResponse, 1)
	rpcRequest := RPCRequest{
		RPCType: MembershipChangeRequest,
		Request: &raftypb.MembershipChangeRequest{
			Id:      member.ID,
			Address: member.Address,
			Action:  uint32(Add),
		},
		ResponseChan: responseChan,
	}

	t.Run("in_progress", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		state.membershipChangeInProgress.Store(true)
		defer state.membershipChangeInProgress.Store(false)
		go state.handleSendMembershipChangeRequest(rpcRequest)
		data := <-responseChan
		assert.Error(data.Error)
		s.wg.Wait()
	})

	t.Run("add_success_false", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		state.handleSendMembershipChangeRequest(rpcRequest)
		s.wg.Wait()
	})

	t.Run("addNode_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)

		follower := &followerReplication{
			Peer:         member,
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		_, err := state.addNode(member, rpcRequest.Request.(*raftypb.MembershipChangeRequest), follower)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("promoteNode_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)

		follower := &followerReplication{
			Peer:         member,
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		_, err := state.promoteNode(Promote, member, follower)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("demote", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		_, err := state.demoteNode(Demote, member)
		assert.Nil(err)
		s.configuration.ServerMembers[0].Decommissioning = true
		_, err = state.demoteNode(Demote, s.configuration.ServerMembers[1])
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("demote_myself", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)

		member := Peer{
			ID:      s.id,
			Address: s.Address.String(),
		}
		_, err := state.demoteNode(Demote, member)
		assert.Nil(err)
		time.Sleep(100 * time.Millisecond)
		assert.Equal(Follower, s.getState())
		s.wg.Wait()
	})

	t.Run("demote_follower", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)

		member := Peer{
			ID:      s.configuration.ServerMembers[0].ID,
			Address: s.configuration.ServerMembers[0].Address,
		}
		follower1 := &followerReplication{
			Peer: Peer{
				ID:      s.configuration.ServerMembers[0].ID,
				Address: s.configuration.ServerMembers[0].Address,
			},
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		follower2 := &followerReplication{
			Peer: Peer{
				ID:      s.configuration.ServerMembers[1].ID,
				Address: s.configuration.ServerMembers[1].Address,
			},
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		state.followerReplication = make(map[string]*followerReplication)
		state.followerReplication[s.configuration.ServerMembers[0].ID] = follower1
		state.followerReplication[s.configuration.ServerMembers[1].ID] = follower2
		_, err := state.demoteNode(Demote, member)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("demote_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		_, err := state.demoteNode(Demote, member)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("removeNode_panic", func(t *testing.T) {
		s := basicNodeSetup()
		assert.Nil(s.logStore.Close())
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		s.configuration.ServerMembers = append(s.configuration.ServerMembers, member)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		_, err := state.removeNode(ForceRemove, member)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("removeNode_return", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		member := Peer{
			ID:              s.configuration.ServerMembers[0].ID,
			Address:         s.configuration.ServerMembers[0].Address,
			Decommissioning: true,
		}
		_, err := state.removeNode(Remove, member)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("removeNode_leaveOnTerminate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		state := leader{rafty: s}
		state.rafty.currentTerm.Store(1)
		member := Peer{
			ID:      s.configuration.ServerMembers[0].ID,
			Address: s.configuration.ServerMembers[0].Address,
		}
		follower1 := &followerReplication{
			Peer: Peer{
				ID:      s.configuration.ServerMembers[0].ID,
				Address: s.configuration.ServerMembers[0].Address,
				address: getNetAddress(s.configuration.ServerMembers[0].Address),
			},
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		follower2 := &followerReplication{
			Peer: Peer{
				ID:      s.configuration.ServerMembers[1].ID,
				Address: s.configuration.ServerMembers[1].Address,
				address: getNetAddress(s.configuration.ServerMembers[1].Address),
			},
			rafty:        s,
			newEntryChan: make(chan *onAppendEntriesRequest, 1),
		}
		state.followerReplication = make(map[string]*followerReplication)
		state.followerReplication[s.configuration.ServerMembers[0].ID] = follower1
		state.followerReplication[s.configuration.ServerMembers[1].ID] = follower2
		go state.followerReplication[s.configuration.ServerMembers[0].ID].startStopFollowerReplication()
		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := state.removeNode(LeaveOnTerminate, member)
		assert.Nil(err)
		s.wg.Wait()
	})
}

func TestMembership_catchupNewMember(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	s.fillIDs()
	s.State = Leader
	s.isRunning.Store(true)

	state := leader{rafty: s}
	state.rafty.currentTerm.Store(1)
	member := Peer{ID: "newbie", Address: "127.0.0.1:60000"}

	currentTerm := s.currentTerm.Load()
	peers, _ := s.getAllPeers()

	peers = append(peers, member)
	action := Add
	nextConfig, _ := state.nextConfiguration(action, peers, member)
	encodedPeers := EncodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(LogConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	s.updateEntriesIndex(entries)
	assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
	request := &onAppendEntriesRequest{
		totalFollowers:             state.totalFollowers.Load(),
		quorum:                     uint64(state.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               s.lastLogIndex.Load(),
		prevLogTerm:                s.lastLogTerm.Load(),
		totalLogs:                  s.lastLogIndex.Load(),
		uuid:                       uuid.NewString(),
		commitIndex:                s.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 time.Second,
		membershipChangeInProgress: &state.membershipChangeInProgress,
		membershipChangeID:         member.ID,
	}

	buildResponse := &raftypb.AppendEntryResponse{
		LogNotFound: true,
	}

	follower := &followerReplication{
		Peer:         member,
		rafty:        s,
		newEntryChan: make(chan *onAppendEntriesRequest, 1),
	}

	t.Run("shutdown", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer s.stopCtx()

		err := follower.catchupNewMember(member, request, buildResponse)
		assert.Error(err)
	})

	s.wg.Wait()
}
