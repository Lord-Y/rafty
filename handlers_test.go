package rafty

import (
	"fmt"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestHandleSendPreVoteRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
	}()
	s.isRunning.Store(true)

	t.Run("granted_false", func(t *testing.T) {
		s.currentTerm.Store(2)
		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: PreVoteRequest,
			Request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.PreVoteResponse)
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})

	t.Run("granted", func(t *testing.T) {
		s.currentTerm.Store(1)
		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: PreVoteRequest,
			Request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.PreVoteResponse)
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("leader", func(t *testing.T) {
		s.currentTerm.Store(1)
		s.setLeader(leaderMap{
			address: s.Address.String(),
			id:      s.id,
		})

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: PreVoteRequest,
			Request: &raftypb.PreVoteRequest{
				Id:          s.id,
				CurrentTerm: 1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendPreVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.PreVoteResponse)
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})
}

func TestHandleSendVoteRequest(t *testing.T) {
	assert := assert.New(t)
	id := 0
	candidateId := fmt.Sprintf("%d", id)

	t.Run("lower", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("lower_panic", func(t *testing.T) {
		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		assert.Nil(s.logStore.Close())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		s.wg.Wait()
	})

	t.Run("candidate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = ""
		s.votedForTerm.Store(0)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(Follower, s.getState())
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("already_voted", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = "1"
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(Follower, s.getState())
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_for_leadership_transfer", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = "1"
		s.votedForTerm.Store(1)
		s.currentTerm.Store(2)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.candidateForLeadershipTransfer.Store(true)

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), uint64(2))
		assert.Equal(Candidate, s.getState())
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_equal_logs", func(t *testing.T) {
		// I'm candidate and the other server too
		// so let's compare logs
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = "1"
		s.votedForTerm.Store(2)
		s.currentTerm.Store(1)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(1)
		entries := []*raftypb.LogEntry{{Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(s.id, response.PeerId)
		assert.Equal(Follower, s.getState())
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_more_logs", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but more logs
		// let's fill other server lastLogIndex etc
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = candidateId
		s.votedForTerm.Store(2)
		s.currentTerm.Store(3)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(1)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      3,
				LastLogTerm:      3,
				LastLogIndex:     2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_with_more_logs_panic", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but more logs
		// let's fill other server lastLogIndex etc
		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.votedFor = candidateId
		s.votedForTerm.Store(2)
		s.currentTerm.Store(3)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(1)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		assert.Nil(s.logStore.Close())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      3,
				LastLogTerm:      3,
				LastLogIndex:     2,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		s.wg.Wait()
	})

	t.Run("candidate_with_more_logs2", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but I have more logs
		// let's fill other server lastLogIndex etc
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = candidateId
		s.votedForTerm.Store(2)
		s.currentTerm.Store(3)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.lastLogIndex.Store(2)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 2}, {Term: 3}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      3,
				LastLogTerm:      3,
				LastLogIndex:     1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_4", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but 0 logs
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = candidateId
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(true, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_4_panic", func(t *testing.T) {
		// I'm candidate and the other server too
		// with same current term but 0 logs
		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.votedFor = candidateId
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		assert.Nil(s.logStore.Close())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		s.wg.Wait()
	})

	t.Run("candidate_5", func(t *testing.T) {
		// I'm candidate and I receive send vote request
		// from other nodes
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.isRunning.Store(true)
		s.votedFor = s.configuration.ServerMembers[1].ID
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendVoteRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.VoteResponse)
		assert.Equal(s.currentTerm.Load(), response.CurrentTerm)
		assert.Equal(false, response.Granted)
		s.wg.Wait()
	})

	t.Run("candidate_5", func(t *testing.T) {
		// I'm candidate and I receive send vote request
		// from other nodes
		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.votedFor = s.configuration.ServerMembers[1].ID
		s.votedForTerm.Store(1)
		s.currentTerm.Store(1)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		assert.Nil(s.logStore.Close())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: VoteRequest,
			Request: &raftypb.VoteRequest{
				CandidateId:      candidateId,
				CandidateAddress: s.configuration.ServerMembers[id].address.String(),
				CurrentTerm:      1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendVoteRequest(request)
		}()
		s.wg.Wait()
	})
}

func TestHandleSendAppendEntriesRequest(t *testing.T) {
	assert := assert.New(t)

	t.Run("my_current_term_greater", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          1,
			},
			ResponseChan: responseChan,
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		s.wg.Wait()
	})

	t.Run("i_am_a_candidate", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		assert.Equal(s.State, Follower)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_only", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_only_panic", func(t *testing.T) {
		s := basicNodeSetup()
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.timer = time.NewTicker(s.heartbeatTimeout())
		assert.Nil(s.logStore.Close())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered. Error:\n", r)
				}
			}()
			s.handleSendAppendEntriesRequest(request)
		}()
		s.wg.Wait()
	})

	t.Run("we_are_both_leaders", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Leader, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_heartbeat_logs_not_found", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.setLeader(leaderMap{
			address: s.configuration.ServerMembers[id].address.String(),
			id:      s.configuration.ServerMembers[id].ID,
		})
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
				Heartbeat:     true,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(true, response.LogNotFound)
		s.wg.Wait()
	})

	t.Run("conflicted_logs_index_out_of_range", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:          idx,
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
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		time.Sleep(100 * time.Millisecond)
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_but_no_matching_logs", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(2)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:          idx,
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
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(false, response.Success)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_matching_logs_only", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		now := time.Now().Unix()
		entries := []*raftypb.LogEntry{{Term: 1, Timestamp: uint32(now)}, {Term: 1, Timestamp: uint32(now), Index: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:          idx,
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
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		time.Sleep(100 * time.Millisecond)
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		assert.Equal(true, response.Success)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_matching_logs_conflict", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 1}, {Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.timer = time.NewTicker(s.heartbeatTimeout())
		peers, _ := s.getPeers()
		peers = append(peers, Peer{Address: "127.0.0.1:60000", ID: "xyz"})
		encodedPeers := encodePeers(peers)
		assert.NotNil(encodedPeers)
		responseChan := make(chan RPCResponse, 1)
		leaderCommitIndex := uint64(5)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      3,
				PrevLogTerm:       2,
				LeaderCommitIndex: leaderCommitIndex,
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
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		assert.Equal(true, response.Success)
		assert.Equal(s.commitIndex.Load(), leaderCommitIndex)
		s.wg.Wait()
	})

	t.Run("he_is_a_leader_with_fake_peers", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Follower, stepUp, false, s.currentTerm.Load())
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}, {Term: 1}, {Term: 2}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.timer = time.NewTicker(s.heartbeatTimeout())
		responseChan := make(chan RPCResponse, 1)
		leaderCommitIndex := uint64(5)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:          idx,
				LeaderAddress:     s.configuration.ServerMembers[id].address.String(),
				Term:              2,
				PrevLogIndex:      3,
				PrevLogTerm:       2,
				LeaderCommitIndex: leaderCommitIndex,
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
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(s.currentTerm.Load(), response.Term)
		assert.Equal(true, response.Success)
		assert.Equal(s.commitIndex.Load(), leaderCommitIndex)
		s.wg.Wait()
	})

	t.Run("candidate_for_leadership_transfer", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
		}()
		s.currentTerm.Store(1)
		s.isRunning.Store(true)
		s.switchState(Candidate, stepUp, false, s.currentTerm.Load())
		s.candidateForLeadershipTransfer.Store(true)
		id := 0
		idx := fmt.Sprintf("%d", id)
		entries := []*raftypb.LogEntry{{Term: 1}}
		s.updateEntriesIndex(entries)
		assert.Nil(s.logStore.StoreLogs(makeLogEntries(entries)))
		s.setLeader(leaderMap{
			address: s.configuration.ServerMembers[id].address.String(),
			id:      s.configuration.ServerMembers[id].ID,
		})
		s.timer = time.NewTicker(s.heartbeatTimeout())

		responseChan := make(chan RPCResponse, 1)
		request := RPCRequest{
			RPCType: AppendEntryRequest,
			Request: &raftypb.AppendEntryRequest{
				LeaderId:      idx,
				LeaderAddress: s.configuration.ServerMembers[id].address.String(),
				Term:          2,
				Heartbeat:     true,
			},
			ResponseChan: responseChan,
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleSendAppendEntriesRequest(request)
		}()
		data := <-responseChan
		response := data.Response.(*raftypb.AppendEntryResponse)
		assert.Equal(false, response.Success)
		s.wg.Wait()
	})
}
