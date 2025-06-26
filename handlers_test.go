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
	"github.com/stretchr/testify/assert"
)

func TestHandleSendPreVoteRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.isRunning.Store(true)

	t.Run("granted_false", func(t *testing.T) {
		s.currentTerm.Store(2)
		responseChan := make(chan *raftypb.PreVoteResponse, 1)
		request := preVoteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(false, data.Granted)
		s.wg.Wait()
	})

	t.Run("granted", func(t *testing.T) {
		s.currentTerm.Store(1)
		responseChan := make(chan *raftypb.PreVoteResponse, 1)
		request := preVoteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})

	t.Run("leader", func(t *testing.T) {
		s.currentTerm.Store(1)
		s.setLeader(leaderMap{
			address: s.Address.String(),
			id:      s.id,
		})

		responseChan := make(chan *raftypb.PreVoteResponse, 1)
		request := preVoteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(false, data.Granted)
		s.wg.Wait()
	})
}

func TestHandleSendVoteRequest(t *testing.T) {
	assert := assert.New(t)
	id := 0
	candidateId := fmt.Sprintf("%d", id)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	s.isRunning.Store(true)

	t.Run("lower", func(t *testing.T) {
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate", func(t *testing.T) {
		s.votedFor = ""
		s.votedForTerm.Store(0)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(Follower, s.getState())
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})

	t.Run("already_voted", func(t *testing.T) {
		s.votedFor = "1"
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(Follower, s.getState())
		assert.Equal(false, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_for_leadership_transfer", func(t *testing.T) {
		s.votedFor = "1"
		s.votedForTerm.Store(1)
		s.currentTerm.Store(2)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.candidateForLeadershipTransfer.Store(true)

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), uint64(2))
		assert.Equal(Candidate, s.getState())
		assert.Equal(false, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_equal_logs", func(t *testing.T) {
		// I'm candidate and the other server too
		// so let's compare logs
		s.votedFor = "1"
		s.votedForTerm.Store(2)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(1)
		s.logs.log = nil
		s.logs.log = append(s.logs.log, &raftypb.LogEntry{Term: 2})

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(s.id, data.PeerID)
		assert.Equal(Follower, s.getState())
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_more_logs", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but more logs
		// let's fill other server lastLogIndex etc
		s.votedFor = candidateId
		s.votedForTerm.Store(2)
		s.currentTerm.Store(3)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(1)
		s.logs.log = nil
		s.logs.log = append(s.logs.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2})
		s.candidateForLeadershipTransfer.Store(false)

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      3,
				LastLogTerm:      3,
				LastLogIndex:     2,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_more_logs2", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but I have more logs
		// let's fill other server lastLogIndex etc
		s.votedFor = candidateId
		s.votedForTerm.Store(2)
		s.currentTerm.Store(3)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(2)
		s.logs.log = nil
		s.logs.log = append(s.logs.log, &raftypb.LogEntry{Term: 1}, &raftypb.LogEntry{Term: 2}, &raftypb.LogEntry{Term: 3})

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      3,
				LastLogTerm:      3,
				LastLogIndex:     1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(false, data.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_4", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but 0 logs
		s.votedFor = candidateId
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.logs.log = nil

		responseChan := make(chan *raftypb.VoteResponse, 1)
		request := voteResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.CurrentTerm)
		assert.Equal(true, data.Granted)
		s.wg.Wait()
	})
}

func TestHandleSendAppendEntriesRequest(t *testing.T) {
	assert := assert.New(t)

	t.Run("my_current_term_greater", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          1,
			},
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		s.wg.Wait()
	})

	t.Run("i_am_a_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		assert.Equal(s.State, Follower)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_only", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		s.wg.Wait()
	})

	t.Run("we_are_both_leaders", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Leader, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_heartbeat_logs_not_found", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}}
		_ = s.logs.appendEntries(entries, false)
		s.setLeader(leaderMap{
			address: s.configuration.ServerMembers[id].address.String(),
			id:      s.configuration.ServerMembers[id].ID,
		})
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
				Heartbeat:     true,
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(true, data.LogNotFound)
		s.wg.Wait()
	})

	t.Run("conflicted_logs_index_out_of_range", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}}
		_ = s.logs.appendEntries(entries, false)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      0,
				PrevLogTerm:       1,
				LeaderCommitIndex: 2,
				Entries: []*raftypb.LogEntry{
					{
						Term:      2,
						Timestamp: uint32(time.Now().Unix()),
					},
				},
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_but_no_matching_logs", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 2}}
		_ = s.logs.appendEntries(entries, false)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      1,
				PrevLogTerm:       1,
				LeaderCommitIndex: 2,
				Entries: []*raftypb.LogEntry{
					{
						Term:      2,
						Timestamp: uint32(time.Now().Unix()),
					},
				},
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(false, data.Success)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_matching_logs_only", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		now := time.Now().Unix()
		entries := []*raftypb.LogEntry{{Term: 1, Timestamp: uint32(now)}, {Term: 1, Timestamp: uint32(now), Index: 1}}
		_ = s.logs.appendEntries(entries, false)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              1,
				PrevLogIndex:      1,
				PrevLogTerm:       1,
				LeaderCommitIndex: 2,
				Entries: []*raftypb.LogEntry{
					{
						Term:      1,
						Timestamp: uint32(now),
					},
					{
						Term:      1,
						Timestamp: uint32(now),
						Index:     1,
					},
				},
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		assert.Equal(true, data.Success)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_matching_logs_conflict", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 1}, {Term: 2}}
		_ = s.logs.appendEntries(entries, false)
		s.timer = time.NewTicker(s.heartbeatTimeout())
		peers, _ := s.getPeers()
		peers = append(peers, peer{Address: "127.0.0.1:6000", ID: "xyz"})
		encodedPeers, err := encodePeers(peers)
		assert.Nil(err)
		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      3,
				PrevLogTerm:       2,
				LeaderCommitIndex: 5,
				Catchup:           true,
				Entries: []*raftypb.LogEntry{
					{
						Term:      1,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      1,
						Index:     1,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     2,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     3,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     4,
						LogType:   uint32(logConfiguration),
						Timestamp: uint32(time.Now().Unix()),
						Command:   encodedPeers,
					},
				},
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		assert.Equal(true, data.Success)
		assert.Equal(s.commitIndex.Load(), request.request.LeaderCommitIndex)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_fake_peers", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 1}, {Term: 2}}
		_ = s.logs.appendEntries(entries, false)
		s.timer = time.NewTicker(s.heartbeatTimeout())
		assert.Nil(err)
		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      3,
				PrevLogTerm:       2,
				LeaderCommitIndex: 5,
				Catchup:           true,
				Entries: []*raftypb.LogEntry{
					{
						Term:      1,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      1,
						Index:     1,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     2,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     3,
						Timestamp: uint32(time.Now().Unix()),
					},
					{
						Term:      2,
						Index:     4,
						LogType:   uint32(logConfiguration),
						Timestamp: uint32(time.Now().Unix()),
						Command:   []byte(`a=b`),
					},
				},
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(s.currentTerm.Load(), data.Term)
		assert.Equal(true, data.Success)
		assert.Equal(s.commitIndex.Load(), request.request.LeaderCommitIndex)
		s.wg.Wait()
	})

	t.Run("candidate_for_leadership_transfer", func(t *testing.T) {
		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)

		s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.candidateForLeadershipTransfer.Store(true)
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.logs.log = nil
		entries := []*raftypb.LogEntry{{Term: 1}}
		_ = s.logs.appendEntries(entries, false)
		s.setLeader(leaderMap{
			address: s.configuration.ServerMembers[id].address.String(),
			id:      s.configuration.ServerMembers[id].ID,
		})
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan *raftypb.AppendEntryResponse, 1)
		request := appendEntriesResquestWrapper{
			responseChan: responseChan,
			request: &raftypb.AppendEntryRequest{
				LeaderID:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
				Heartbeat:     true,
			},
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		assert.Equal(false, data.Success)
		s.wg.Wait()
	})
}
