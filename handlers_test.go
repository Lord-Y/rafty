package rafty

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestHandleSendPreVoteRequestReader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.CurrentTerm = 1

	go s.handleSendPreVoteRequestReader()
	data := <-s.rpcPreVoteRequestChanWritter
	assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
	assert.Equal(s.ID, data.GetPeerID())
}

func TestHandlePreVoteResponseError(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.CurrentTerm = 1
	err = errors.New("testError")

	s.handlePreVoteResponseError(voteResponseErrorWrapper{
		peer: s.configuration.ServerMembers[0],
		err:  err,
	})
}

func TestHandlePreVoteResponse(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.CurrentTerm = 1
	s.State = Follower
	s.MinimumClusterSize = 3

	s.handlePreVoteResponse(preVoteResponseWrapper{
		peer: s.configuration.ServerMembers[0],
		response: &raftypb.PreVoteResponse{
			PeerID:      s.configuration.ServerMembers[0].ID,
			State:       Follower.String(),
			CurrentTerm: 2,
		},
	})
	assert.Equal(uint64(2), s.CurrentTerm)
	assert.Equal(Follower, s.getState())

	// reset pre candidate peers
	s.configuration.preCandidatePeers = nil
	s.CurrentTerm = 1
	s.State = Follower
	for i := 0; i < len(s.configuration.ServerMembers); i++ {
		s.handlePreVoteResponse(preVoteResponseWrapper{
			peer: s.configuration.ServerMembers[i],
			response: &raftypb.PreVoteResponse{
				PeerID:      s.configuration.ServerMembers[i].ID,
				State:       Follower.String(),
				CurrentTerm: 1,
			},
		})
		assert.Equal(uint64(1), s.CurrentTerm)
	}

	// leader lost
	s.leaderLost.Store(true)
	s.configuration.preCandidatePeers = nil
	s.CurrentTerm = 1
	s.State = Follower
	for i := 0; i < len(s.configuration.ServerMembers); i++ {
		s.handlePreVoteResponse(preVoteResponseWrapper{
			peer: s.configuration.ServerMembers[i],
			response: &raftypb.PreVoteResponse{
				PeerID:      s.configuration.ServerMembers[i].ID,
				State:       Follower.String(),
				CurrentTerm: 1,
			},
		})
		assert.Equal(uint64(1), s.CurrentTerm)
	}
}

func TestHandleSendVoteRequestReader(t *testing.T) {
	assert := assert.New(t)
	id := 0
	candidateId := fmt.Sprintf("%d", id)
	t.Run("basic", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.CurrentTerm = 4
		s.State = Follower

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      2,
		})
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
	})

	t.Run("candidate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.votedFor = ""
		s.votedForTerm = 0
		s.State = Candidate
		s.CurrentTerm = 1

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      2,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(Follower, s.getState())
	})

	t.Run("already_voted", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// already voted
		s.votedFor = "1"
		s.votedForTerm = 1
		s.State = Follower
		s.CurrentTerm = 1

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      1,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(Follower, s.getState())
	})

	t.Run("candidate_0", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// I'm candidate and the other server too
		// so let's compare logs
		s.votedFor = "1"
		s.votedForTerm = 2
		s.State = Follower
		s.CurrentTerm = 1
		s.lastLogIndex = 1
		s.log = nil
		s.log = append(s.log, &raftypb.LogEntry{Term: 2})

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      2,
		})
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(Follower, s.getState())
		assert.Equal(true, data.GetVoteGranted())
	})

	t.Run("candidate_1", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// I'm candidate and the other server too
		// with same current term but more logs
		// let's fill other server lastLogIndex etc
		s.votedFor = candidateId
		s.votedForTerm = 2
		s.State = Candidate
		s.CurrentTerm = 3
		s.lastLogIndex = 1
		s.log = nil
		s.log = append(s.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2})

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      3,
			LastLogTerm:      3,
			LastLogIndex:     2,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(true, data.GetVoteGranted())
	})

	t.Run("candidate_2", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// I'm candidate and the other server too
		// with same current term but I have more logs
		// let's fill other server lastLogIndex etc
		s.votedFor = candidateId
		s.votedForTerm = 2
		s.State = Candidate
		s.CurrentTerm = 3
		s.lastLogIndex = 2
		s.log = nil
		s.log = append(s.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2}, &raftypb.LogEntry{Term: 3})

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      3,
			LastLogTerm:      3,
			LastLogIndex:     1,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(false, data.GetVoteGranted())
	})

	t.Run("candidate_4", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// I'm candidate and the other server too
		// with same current term but 0 logs
		s.votedFor = candidateId
		s.votedForTerm = 1
		s.State = Candidate
		s.CurrentTerm = 1
		s.log = nil

		go s.handleSendVoteRequestReader(&raftypb.VoteRequest{
			CandidateId:      candidateId,
			CandidateAddress: s.configuration.ServerMembers[id].address.String(),
			State:            Candidate.String(),
			CurrentTerm:      1,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendVoteRequestChanWritter
		assert.Equal(s.CurrentTerm, data.GetCurrentTerm())
		assert.Equal(s.ID, data.GetPeerID())
		assert.Equal(true, data.GetVoteGranted())
	})
}

func TestHandleVoteResponseError(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.CurrentTerm = 1
	err = errors.New("testError")

	s.handleVoteResponseError(voteResponseErrorWrapper{
		peer: s.configuration.ServerMembers[0],
		err:  err,
	})
}

func TestHandleVoteResponse(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.State = Follower

	// my term is lower this other node term
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.handleVoteResponse(voteResponseWrapper{
		peer:             s.configuration.ServerMembers[0],
		savedCurrentTerm: 1,
		response: &raftypb.VoteResponse{
			CurrentTerm: 2,
		},
	})

	// my term is greater this other node term
	// but it detected a new leader
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.handleVoteResponse(voteResponseWrapper{
		peer:             s.configuration.ServerMembers[0],
		savedCurrentTerm: 2,
		response: &raftypb.VoteResponse{
			CurrentTerm:       1,
			NewLeaderDetected: true,
		},
	})

	// my term is greater this other node term
	// but it granted my vote
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.handleVoteResponse(voteResponseWrapper{
		peer:             s.configuration.ServerMembers[0],
		savedCurrentTerm: 2,
		response: &raftypb.VoteResponse{
			CurrentTerm: 1,
			VoteGranted: true,
		},
	})

	// my term is greater this other node term
	// but it didn't granted my vote
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.handleVoteResponse(voteResponseWrapper{
		peer:             s.configuration.ServerMembers[0],
		savedCurrentTerm: 2,
		response: &raftypb.VoteResponse{
			CurrentTerm: 1,
		},
	})

	// my term is greater this other node term
	// but it asked me to step down
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.handleVoteResponse(voteResponseWrapper{
		peer:             s.configuration.ServerMembers[0],
		savedCurrentTerm: 2,
		response: &raftypb.VoteResponse{
			CurrentTerm:       1,
			RequesterStepDown: true,
		},
	})

	// my term is greater this other node term
	// and I'm a candidate
	s.quoroms = nil
	s.configuration.preCandidatePeers = nil
	s.State = Candidate
	for i := 0; i < len(s.configuration.ServerMembers); i++ {
		s.handleVoteResponse(voteResponseWrapper{
			peer:             s.configuration.ServerMembers[1],
			savedCurrentTerm: 2,
			response: &raftypb.VoteResponse{
				CurrentTerm: 1,
			},
		})
	}
}

func TestHandleClientGetLeaderReader(t *testing.T) {
	assert := assert.New(t)
	t.Run("no_leader", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.CurrentTerm = 2

		// no leader
		go s.handleClientGetLeaderReader()
		time.Sleep(time.Second)
		data := <-s.rpcClientGetLeaderChanWritter
		assert.Equal("", data.GetLeaderID())
		assert.Equal("", data.GetLeaderAddress())
	})

	t.Run("leader", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// I am the leader
		s.State = Leader
		go s.handleClientGetLeaderReader()
		time.Sleep(time.Second)
		data := <-s.rpcClientGetLeaderChanWritter
		assert.Equal(s.ID, data.GetLeaderID())
		assert.Equal(s.Address.String(), data.GetLeaderAddress())
	})

	t.Run("we_have_a_leader", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		// we have a leader
		s.State = Follower
		time.Sleep(2 * time.Second)
		s.leader = &leaderMap{
			address: s.configuration.ServerMembers[0].address.String(),
			id:      s.configuration.ServerMembers[0].ID,
		}
		go s.handleClientGetLeaderReader()
		time.Sleep(time.Second)
		data := <-s.rpcClientGetLeaderChanWritter
		assert.Equal(s.configuration.ServerMembers[0].ID, data.GetLeaderID())
		assert.Equal(s.configuration.ServerMembers[0].address.String(), data.GetLeaderAddress())
	})
}

func TestHandleSendAppendEntriesRequestReader(t *testing.T) {
	assert := assert.New(t)
	t.Run("my_current_term_greater", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 2
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:      idx,
			LeaderAddress: s.configuration.ServerMembers[id].address.String(),
			Term:          1,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("i_am_a_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Candidate
		s.CurrentTerm = 1
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:      idx,
			LeaderAddress: s.configuration.ServerMembers[id].address.String(),
			Term:          2,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("he_is_a_leader", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 2
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:      idx,
			LeaderAddress: s.configuration.ServerMembers[id].address.String(),
			Term:          2,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("we_are_both_leaders", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Leader
		s.CurrentTerm = 2
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:      idx,
			LeaderAddress: s.configuration.ServerMembers[id].address.String(),
			Term:          2,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("he_is_a_leader_with_heartbeat_logs", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 1
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		s.log = append(s.log, &raftypb.LogEntry{Term: 1})
		s.leader = &leaderMap{
			address: s.configuration.ServerMembers[id].address.String(),
			id:      s.configuration.ServerMembers[id].ID,
		}
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:      idx,
			LeaderAddress: s.configuration.ServerMembers[id].address.String(),
			Term:          1,
			Heartbeat:     true,
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("he_is_a_leader_with_logs_0", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 1
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		s.log = append(s.log, &raftypb.LogEntry{Term: 1})
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:          idx,
			LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
			Term:              2,
			PrevLogIndex:      0,
			PrevLogTerm:       1,
			LeaderCommitIndex: 2,
			Entries: []*raftypb.LogEntry{
				{
					Term:      2,
					TimeStamp: uint32(time.Now().Unix()),
				},
			},
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("he_is_a_leader_but_no_matching_logs", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 2
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		s.log = append(s.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2})
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:          idx,
			LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
			Term:              2,
			PrevLogIndex:      1,
			PrevLogTerm:       1,
			LeaderCommitIndex: 2,
			Entries: []*raftypb.LogEntry{
				{
					Term:      2,
					TimeStamp: uint32(time.Now().Unix()),
				},
			},
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(false, data.GetSuccess())
	})

	t.Run("he_is_a_leader_with_matching_logs", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 1
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		s.log = append(s.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 1})
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:          idx,
			LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
			Term:              1,
			PrevLogIndex:      1,
			PrevLogTerm:       1,
			LeaderCommitIndex: 2,
			Entries: []*raftypb.LogEntry{
				{
					Term:      1,
					TimeStamp: uint32(time.Now().Unix()),
				},
				{
					Term:      1,
					TimeStamp: uint32(time.Now().Unix()),
				},
			},
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
	})

	t.Run("he_is_a_leader_with_matching_logs_conflict", func(t *testing.T) {
		os.Setenv("RAFTY_LOG_LEVEL", "trace")
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.State = Follower
		s.CurrentTerm = 1
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.electionTimer = time.NewTimer(s.randomElectionTimeout())
		s.log = append(s.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2})
		go s.handleSendAppendEntriesRequestReader(&raftypb.AppendEntryRequest{
			LeaderID:          idx,
			LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
			Term:              2,
			PrevLogIndex:      1,
			PrevLogTerm:       1,
			LeaderCommitIndex: 4,
			Entries: []*raftypb.LogEntry{
				{
					Term:      1,
					TimeStamp: uint32(time.Now().Unix()),
				},
				{
					Term:      1,
					TimeStamp: uint32(time.Now().Unix()),
				},
				{
					Term:      2,
					TimeStamp: uint32(time.Now().Unix()),
				},
				{
					Term:      2,
					TimeStamp: uint32(time.Now().Unix()),
				},
				{
					Term:      2,
					TimeStamp: uint32(time.Now().Unix()),
				},
			},
		})
		time.Sleep(time.Second)
		data := <-s.rpcSendAppendEntriesRequestChanWritter
		assert.Equal(s.getCurrentTerm(), data.GetTerm())
		os.Unsetenv("RAFTY_LOG_LEVEL")
	})
}
