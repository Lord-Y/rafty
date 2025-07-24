package rafty

import (
	"fmt"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// handleSendPreVoteRequest allow the current node to manage
// pre vote requests coming from other nodes
func (r *Rafty) handleSendPreVoteRequest(data RPCRequest) {
	request := data.Request.(*raftypb.PreVoteRequest)
	leader := r.getLeader()
	currentTerm := r.currentTerm.Load()
	response := &raftypb.PreVoteResponse{
		PeerId:      r.id,
		CurrentTerm: r.currentTerm.Load(),
	}
	rpcResponse := RPCResponse{
		Response: response,
	}

	switch {
	case leader != (leaderMap{}):
		response.Granted = false
	case currentTerm > request.CurrentTerm:
		response.Granted = false
	default:
		response.Granted = true
	}
	data.ResponseChan <- rpcResponse
}

// handleSendVoteRequest allow the current node to manage
// vote requests coming from other nodes
func (r *Rafty) handleSendVoteRequest(data RPCRequest) {
	request := data.Request.(*raftypb.VoteRequest)
	currentTerm := r.currentTerm.Load()
	votedFor, _ := r.getVotedFor()
	lastLogIndex := r.lastLogIndex.Load()
	totalLogs := r.logs.total().total
	response := &raftypb.VoteResponse{
		PeerId: r.id,
	}
	rpcResponse := RPCResponse{
		Response: response,
	}

	// if my current term is lower than candidate current term
	// set my current term to the candidate term
	// vote for the candidate
	if currentTerm < request.CurrentTerm {
		r.currentTerm.Store(request.CurrentTerm)
		r.setVotedFor(request.CandidateId, request.CurrentTerm)

		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Vote granted to peer")

		r.switchState(Follower, stepDown, true, request.CurrentTerm)
		if err := r.storage.metadata.store(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Fail to persist metadata")
		}
		response.CurrentTerm = request.CurrentTerm
		response.Granted = true
		data.ResponseChan <- rpcResponse
		return
	}

	if r.candidateForLeadershipTransfer.Load() {
		response.Granted = false
		data.ResponseChan <- rpcResponse

		r.Logger.Warn().Err(ErrLeadershipTransferInProgress).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Rejecting vote request because of leadership transfer")
		return
	}

	if votedFor != "" && votedFor != request.CandidateId {
		response.CurrentTerm = currentTerm
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Vote request rejected to peer because I already voted")

		data.ResponseChan <- rpcResponse
		return
	}

	if totalLogs > 0 {
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
			Str("lastLogIndex", fmt.Sprintf("%d", r.lastLogIndex.Load())).
			Msgf("debug data lastLogTerm")

		lastLogTerm := r.logs.fromIndex(r.lastLogIndex.Load())
		if lastLogTerm.err != nil {
			r.Logger.Error().Err(lastLogTerm.err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
				Str("lastLogIndex", fmt.Sprintf("%d", r.lastLogIndex.Load())).
				Msgf("Fail to get last log term")
			response.Granted = false
			data.ResponseChan <- rpcResponse
			return
		}

		lastLogTermData := r.logs.fromIndex(r.lastLogIndex.Load()).logs[0].Term
		if request.LastLogTerm > lastLogTermData || (lastLogTermData == request.LastLogTerm && request.LastLogIndex >= lastLogIndex) {
			r.setVotedFor(request.CandidateId, request.CurrentTerm)
			response.CurrentTerm = request.CurrentTerm
			response.Granted = true
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", request.CandidateAddress).
				Str("peerId", request.CandidateId).
				Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
				Msgf("Vote granted to peer")

			r.switchState(Follower, stepDown, false, request.CurrentTerm)
			if err := r.storage.metadata.store(); err != nil {
				r.Logger.Fatal().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Fail to persist metadata")
			}
			data.ResponseChan <- rpcResponse
			return
		}

		response.CurrentTerm = currentTerm
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("peerLastLogIndex", fmt.Sprintf("%d", request.LastLogIndex)).
			Msgf("Vote request rejected to peer because of lastLogIndex")

		data.ResponseChan <- rpcResponse
		return
	}

	r.setVotedFor(request.CandidateId, request.CurrentTerm)
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", currentTerm)).
		Str("peerAddress", request.CandidateAddress).
		Str("peerId", request.CandidateId).
		Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
		Msgf("Vote granted to peer")

	r.switchState(Follower, stepDown, false, request.CurrentTerm)
	if err := r.storage.metadata.store(); err != nil {
		r.Logger.Fatal().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to persist metadata")
	}

	response.CurrentTerm = request.CurrentTerm
	response.Granted = true
	data.ResponseChan <- rpcResponse
}

// handleSendAppendEntriesRequest allow the current node to manage
// vote requests coming from other nodes
func (r *Rafty) handleSendAppendEntriesRequest(data RPCRequest) {
	// if our local term is greater than leader term
	// reply false §5.1
	request := data.Request.(*raftypb.AppendEntryRequest)
	currentTerm := r.currentTerm.Load()
	lastLogIndex := r.lastLogIndex.Load()
	lastLogTerm := r.lastLogTerm.Load()
	response := &raftypb.AppendEntryResponse{
		Term:         currentTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rpcResponse := RPCResponse{
		Response: response,
	}

	if !r.options.ReadReplica {
		if currentTerm > request.Term {
			data.ResponseChan <- rpcResponse

			r.Logger.Warn().Err(ErrTermTooOld).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Msgf("My term is higher than peer")
			return
		}

		if r.candidateForLeadershipTransfer.Load() {
			data.ResponseChan <- rpcResponse

			r.Logger.Warn().Err(ErrLeadershipTransferInProgress).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Msgf("Rejecting append entries because of leadership transfer")
			return
		}

		if request.Term > currentTerm {
			r.Logger.Warn().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Msgf("My term is lower than peer")
		}
		r.switchState(Follower, stepDown, true, request.Term)
	}

	r.currentTerm.Store(request.Term)
	if err := r.storage.metadata.store(); err != nil {
		r.Logger.Fatal().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to persist metadata")
	}

	r.setLeader(leaderMap{
		id:      request.LeaderId,
		address: request.LeaderAddress,
	})
	r.leaderLastContactDate.Store(time.Now())
	r.timer.Reset(r.heartbeatTimeout())

	totalLogs := r.logs.total().total
	if (request.PrevLogIndex != lastLogIndex || request.PrevLogTerm != int64(lastLogTerm)) && !request.Catchup {
		response.LogNotFound = true
		data.ResponseChan <- rpcResponse

		if r.getState() != Down {
			r.Logger.Warn().Err(ErrLogNotFound).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
				Str("leaderPrevLogTerm", fmt.Sprintf("%d", request.PrevLogTerm)).
				Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
				Msgf("Previous log not found")
		}
		return
	}

	if !request.Heartbeat {
		var newEntries []*raftypb.LogEntry
		for index, entry := range request.Entries {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
				Str("leaderNewTotalLogs", fmt.Sprintf("%d", len(request.Entries))).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("index", fmt.Sprintf("%d", index)).
				Str("entryIndex", fmt.Sprintf("%d", entry.Index)).
				Msgf("debug data received append entries")

			if entry.Index > lastLogIndex || totalLogs == 0 {
				newEntries = request.Entries[index:]
				break
			}

			if totalLogs > 0 {
				lastLog := r.logs.fromIndex(entry.Index)
				if entry.Term != lastLog.logs[0].Term {
					if wipe := r.logs.wipeEntries(entry.Index, lastLogIndex); wipe.err != nil {
						r.Logger.Error().Err(wipe.err).
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("state", r.getState().String()).
							Str("term", fmt.Sprintf("%d", currentTerm)).
							Str("rangeFrom", fmt.Sprintf("%d", entry.Index)).
							Str("rangeTo", fmt.Sprintf("%d", lastLogIndex)).
							Str("leaderAddress", request.LeaderAddress).
							Str("leaderId", request.LeaderId).
							Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
							Msgf("Fail to remove conflicting log from range")
						data.ResponseChan <- rpcResponse
						return
					}

					newEntries = request.Entries[index:]
					break
				}
			}
		}

		if lenEntries := len(newEntries); lenEntries > 0 {
			var (
				err error
			)
			totalLogs = r.logs.appendEntries(newEntries, false)
			for index := range newEntries {
				entryIndex := 0
				if totalLogs > 0 {
					entryIndex = int(totalLogs) - 1 + index
				}
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("term", fmt.Sprintf("%d", currentTerm)).
					Str("entryIndex", fmt.Sprintf("%d", entryIndex)).
					Str("index", fmt.Sprintf("%d", index)).
					Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
					Msgf("debug data persistance")

				if err = r.logs.applyConfigEntry(newEntries[index]); err != nil {
					r.Logger.Warn().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("leaderAddress", request.LeaderAddress).
						Str("leaderId", request.LeaderId).
						Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
						Msgf("Fail to apply config entry")
				}

				if err := r.storage.metadata.store(); err != nil {
					r.Logger.Fatal().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("leaderAddress", request.LeaderAddress).
						Str("leaderId", request.LeaderId).
						Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
						Msgf("Fail to persist metadata")
				}

				if err := r.storage.data.store(newEntries[index]); err != nil {
					r.Logger.Fatal().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Msgf("Fail to persist data")
				}
			}

			if request.LeaderCommitIndex > r.commitIndex.Load() {
				r.commitIndex.Store(min(request.LeaderCommitIndex, uint64(totalLogs)))
			}
			r.lastApplied.Store(uint64(totalLogs - 1))

			r.Logger.Debug().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("lastLogIndex", fmt.Sprintf("%d", r.lastLogIndex.Load())).
				Str("matchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
				Str("lastApplied", fmt.Sprintf("%d", r.lastApplied.Load())).
				Str("lastAppliedConfigIndex", fmt.Sprintf("%d", r.lastAppliedConfigIndex.Load())).
				Str("lastAppliedConfigTerm", fmt.Sprintf("%d", r.lastAppliedConfigTerm.Load())).
				Msgf("Node state index updated")

			if err := r.storage.metadata.store(); err != nil {
				r.Logger.Fatal().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Fail to persist metadata")
			}
		}
	}
	response.Term = request.Term
	response.Success = true
	data.ResponseChan <- rpcResponse

	// this is only temporary as we added more debug logs
	if !r.options.ReadReplica {
		r.switchState(Follower, stepDown, false, request.Term)
	}

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", request.Term)).
		Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
		Str("heartbeat", fmt.Sprintf("%t", request.Heartbeat)).
		Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
		Str("lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
		Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
		Str("leaderAddress", request.LeaderAddress).
		Str("leaderId", request.LeaderId).
		Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
		Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
		Str("leaderPrevLogTerm", fmt.Sprintf("%d", request.PrevLogTerm)).
		Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
		Msgf("Received append entries from leader")

	if r.shutdownOnRemove.Load() {
		go r.stop()
	}
}
