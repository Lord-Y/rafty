package rafty

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              peer
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              peer{ID: "newnode"},
				expWaitToBePromoted: true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              peer{ID: "newnode", ReadOnlyNode: true},
				expWaitToBePromoted: true,
				expReadOnlyNode:     true,
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("remove", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              peer
			action              MembershipChange
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              peer{ID: "newnode"},
				action:              Remove,
				expWaitToBePromoted: false,
				expContained:        false,
				expVerify:           true,
			},
			{
				member:              peer{ID: "newnode", ReadOnlyNode: true},
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("promote", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              peer
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              peer{ID: "newnode", ReadOnlyNode: true},
				expWaitToBePromoted: false,
				expDecommissioning:  false,
				expReadOnlyNode:     true,
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("demoted", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              peer
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:              peer{ID: "newnode"},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expContained:        true,
				expVerify:           true,
			},
			{
				member:              peer{ID: "newnode", ReadOnlyNode: true},
				expWaitToBePromoted: false,
				expDecommissioning:  true,
				expReadOnlyNode:     true,
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("all", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			member              peer
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    peer{ID: "newnode"},
				expVerify: true,
			},
			{
				member:    peer{ID: "newnode", ReadOnlyNode: true},
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})

	t.Run("breaking", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		tests := []struct {
			action              MembershipChange
			member              peer
			memberid            int
			expReadOnlyNode     bool
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
				assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
				assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
				assert.Equal(tc.expVerify, state.verifyConfiguration(next))
			}
		}
	})

	t.Run("leaveOnTerminate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.State = Leader

		state := leader{rafty: s}
		peers, _ := s.getAllPeers()
		tests := []struct {
			member              peer
			expReadOnlyNode     bool
			expWaitToBePromoted bool
			expDecommissioning  bool
			expError            error
			expContained        bool
			expVerify           bool
		}{
			{
				member:    peer{ID: "newnode"},
				expVerify: true,
			},
			{
				member:    peer{ID: "newnode", ReadOnlyNode: true},
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
			assert.Equal(tc.expReadOnlyNode, next[len(next)-1].ReadOnlyNode)
			assert.Equal(tc.expContained, isPartOfTheCluster(next, tc.member))
			assert.Equal(tc.expVerify, state.verifyConfiguration(next))
		}
	})
}

func TestMembership_changeRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()
	s.State = Leader
	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	s.isRunning.Store(true)

	state := leader{rafty: s}
	state.rafty.currentTerm.Store(1)
	member := peer{
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
		state.membershipChangeInProgress.Store(true)
		defer state.membershipChangeInProgress.Store(false)
		go state.handleSendMembershipChangeRequest(rpcRequest)
		data := <-responseChan
		assert.Error(data.Error)
	})

	t.Run("add_success_false", func(t *testing.T) {
		state.handleSendMembershipChangeRequest(rpcRequest)
	})

	t.Run("demote", func(t *testing.T) {
		_, err = state.demoteNode(Demote, member)
		assert.Nil(err)
		s.configuration.ServerMembers[0].Decommissioning = true
		_, err = state.demoteNode(Demote, s.configuration.ServerMembers[1])
		assert.Error(err)
	})
	s.wg.Wait()
}

func TestMembership_catchupNewMember(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()
	s.State = Leader
	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	s.isRunning.Store(true)

	state := leader{rafty: s}
	state.rafty.currentTerm.Store(1)
	member := peer{ID: "newbie", Address: "127.0.0.1:60000"}

	currentTerm := s.currentTerm.Load()
	peers, _ := s.getAllPeers()

	peers = append(peers, member)
	action := Add
	nextConfig, _ := state.nextConfiguration(action, peers, member)
	encodedPeers := encodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	totalLogs := s.logs.appendEntries(entries, false)
	request := &onAppendEntriesRequest{
		totalFollowers:             state.totalFollowers.Load(),
		quorum:                     uint64(state.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               s.lastLogIndex.Load(),
		prevLogTerm:                s.lastLogTerm.Load(),
		totalLogs:                  uint64(totalLogs),
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
		peer:                member,
		rafty:               s,
		newEntryChan:        make(chan *onAppendEntriesRequest, 1),
		replicationStopChan: make(chan struct{}, 1),
	}

	t.Run("shutdown", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer s.stopCtx()

		err := follower.catchupNewMember(member, request, buildResponse)
		assert.Error(err)
	})

	s.wg.Wait()
}
