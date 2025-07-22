package rafty

import (
	"context"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestStateLoop_runAsReadReplica(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = ReadReplica
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("membership", func(t *testing.T) {
		go s.runAsReadReplica()
		responseChan := make(chan RPCResponse, 1)
		s.rpcMembershipChangeRequestChan <- RPCRequest{
			RPCType:      MembershipChangeRequest,
			Request:      &raftypb.MembershipChangeRequest{Id: "newnode", Address: "127.0.0.1:60000", Action: uint32(Add)},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrNotLeader, data.Error)
	})
}

func TestStateLoop_runAsFollower(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = Follower
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("membership", func(t *testing.T) {
		s.askForMembershipInProgress.Store(true) // done this on purpose
		go s.runAsFollower()
		responseChan := make(chan RPCResponse, 1)
		s.rpcMembershipChangeRequestChan <- RPCRequest{
			RPCType:      MembershipChangeRequest,
			Request:      &raftypb.MembershipChangeRequest{Id: "newnode", Address: "127.0.0.1:60000", Action: uint32(Add)},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrNotLeader, data.Error)
	})
}

func TestStateLoop_runAsCandidate(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = Candidate
	s.timer = time.NewTicker(s.randomElectionTimeout())

	t.Run("membership", func(t *testing.T) {
		s.askForMembershipInProgress.Store(true) // done this on purpose
		go s.runAsCandidate()
		responseChan := make(chan RPCResponse, 1)
		s.rpcMembershipChangeRequestChan <- RPCRequest{
			RPCType:      MembershipChangeRequest,
			Request:      &raftypb.MembershipChangeRequest{Id: "newnode", Address: "127.0.0.1:60000", Action: uint32(Add)},
			ResponseChan: responseChan,
		}
		data := <-responseChan
		assert.Equal(ErrNotLeader, data.Error)
	})
}

func TestStateLoop_runAsLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.fillIDs()

	s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), 2*time.Second)
	s.isRunning.Store(true)
	s.State = Leader
	s.timer = time.NewTicker(s.randomElectionTimeout())
	s.currentTerm.Store(2)
	id := 1

	t.Run("handleSendVoteRequest", func(t *testing.T) {
		go s.runAsLeader()
		responseChan := make(chan RPCResponse, 1)
		s.rpcVoteRequestChan <- RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      s.configuration.ServerMembers[id].ID,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		data := <-responseChan
		assert.Equal(nil, data.Error)
	})

	t.Run("handleSendAppendEntriesRequest", func(t *testing.T) {
		go s.runAsLeader()
		responseChan := make(chan RPCResponse, 1)
		s.rpcAppendEntriesRequestChan <- RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderID:      s.configuration.ServerMembers[id].ID,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          1,
			},
			ResponseChan: responseChan,
		}

		data := <-responseChan
		assert.Equal(nil, data.Error)
	})
}
