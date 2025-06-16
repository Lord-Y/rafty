package rafty

import (
	"fmt"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *Rafty) handleSendPreVoteRequest(data preVoteResquestWrapper) {
	leader := r.getLeader()
	currentTerm := r.currentTerm.Load()
	response := &raftypb.PreVoteResponse{
		PeerID:      r.id,
		CurrentTerm: r.currentTerm.Load(),
	}

	switch {
	case leader != (leaderMap{}):
		response.Granted = false
	case currentTerm > data.request.CurrentTerm:
		response.Granted = false
	default:
		response.Granted = true
	}
	data.responseChan <- response
}

func (r *Rafty) handleSendVoteRequest(data voteResquestWrapper) {
	currentTerm := r.currentTerm.Load()
	votedFor, _ := r.getVotedFor()
	lastLogIndex := r.lastLogIndex.Load()
	totalLogs := r.logs.total().total
	response := &raftypb.VoteResponse{
		PeerID: r.id,
	}

	// if my current term is lower than candidate current term
	// set my current term to the candidate term
	// vote for the candidate
	if currentTerm < data.request.CurrentTerm {
		r.currentTerm.Store(data.request.CurrentTerm)
		r.setVotedFor(data.request.CandidateId, data.request.CurrentTerm)

		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", data.request.CandidateAddress).
			Str("peerId", data.request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", data.request.CurrentTerm)).
			Msgf("Vote granted to peer")

		r.switchState(Follower, stepDown, true, data.request.CurrentTerm)
		if err := r.storage.metadata.store(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Fail to persist metadata")
		}
		response.CurrentTerm = data.request.CurrentTerm
		response.Granted = true
		data.responseChan <- response
		return
	}

	// need to be reevaluated, not sure it's necessary anymore
	if votedFor != "" && votedFor != data.request.CandidateId {
		response.CurrentTerm = currentTerm
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", data.request.CandidateAddress).
			Str("peerId", data.request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", data.request.CurrentTerm)).
			Msgf("Vote request rejected to peer because I already voted")

		data.responseChan <- response
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

		lastLogTerm := r.logs.fromIndex(r.lastLogIndex.Load()).logs[0].Term
		if data.request.LastLogTerm > lastLogTerm || (lastLogTerm == data.request.LastLogTerm && data.request.LastLogIndex >= lastLogIndex) {
			r.setVotedFor(data.request.CandidateId, data.request.CurrentTerm)
			response.CurrentTerm = data.request.CurrentTerm
			response.Granted = true
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", data.request.CandidateAddress).
				Str("peerId", data.request.CandidateId).
				Str("peerTerm", fmt.Sprintf("%d", data.request.CurrentTerm)).
				Msgf("Vote granted to peer")

			r.switchState(Follower, stepDown, false, data.request.CurrentTerm)
			if err := r.storage.metadata.store(); err != nil {
				r.Logger.Fatal().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Fail to persist metadata")
			}
			data.responseChan <- response
			return
		}

		response.CurrentTerm = currentTerm
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
			Str("peerAddress", data.request.CandidateAddress).
			Str("peerId", data.request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", data.request.CurrentTerm)).
			Str("peerLastLogIndex", fmt.Sprintf("%d", data.request.LastLogIndex)).
			Msgf("Vote request rejected to peer because of lastLogIndex")

		data.responseChan <- response
		return
	}

	r.setVotedFor(data.request.CandidateId, data.request.CurrentTerm)
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", currentTerm)).
		Str("peerAddress", data.request.CandidateAddress).
		Str("peerId", data.request.CandidateId).
		Str("peerTerm", fmt.Sprintf("%d", data.request.CurrentTerm)).
		Msgf("Vote granted to peer")

	r.switchState(Follower, stepDown, false, data.request.CurrentTerm)
	if err := r.storage.metadata.store(); err != nil {
		r.Logger.Fatal().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to persist metadata")
	}

	response.CurrentTerm = data.request.CurrentTerm
	response.Granted = true
	data.responseChan <- response
}

func (r *Rafty) handleSendAppendEntriesRequest(data appendEntriesResquestWrapper) {
	// if our local term is greater than leader term
	// reply false §5.1
	currentTerm := r.currentTerm.Load()
	lastLogIndex := r.lastLogIndex.Load()
	lastLogTerm := r.lastLogTerm.Load()
	response := &raftypb.AppendEntryResponse{
		Term:         currentTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	if !r.options.ReadOnlyNode {
		if currentTerm > data.request.Term {
			data.responseChan <- response

			r.Logger.Warn().Err(ErrTermTooOld).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("leaderAddress", data.request.LeaderAddress).
				Str("leaderId", data.request.LeaderID).
				Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
				Msgf("My term is higher than peer")
			return
		}

		if data.request.Term > currentTerm {
			r.Logger.Warn().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("leaderAddress", data.request.LeaderAddress).
				Str("leaderId", data.request.LeaderID).
				Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
				Msgf("My term is lower than peer")

			r.switchState(Follower, stepDown, true, data.request.Term)
		}
	}

	r.currentTerm.Store(data.request.Term)
	if err := r.storage.metadata.store(); err != nil {
		r.Logger.Fatal().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to persist metadata")
	}

	r.setLeader(leaderMap{
		id:      data.request.LeaderID,
		address: data.request.LeaderAddress,
	})
	r.leaderLastContactDate.Store(time.Now())
	r.timer.Reset(r.heartbeatTimeout())

	totalLogs := r.logs.total().total
	if (data.request.PrevLogIndex != lastLogIndex || data.request.PrevLogTerm != int64(lastLogTerm)) && !data.request.Catchup {
		response.LogNotFound = true
		data.responseChan <- response

		if r.getState() != Down {
			r.Logger.Warn().Err(ErrLogNotFound).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("leaderAddress", data.request.LeaderAddress).
				Str("leaderId", data.request.LeaderID).
				Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", data.request.PrevLogIndex)).
				Str("leaderPrevLogTerm", fmt.Sprintf("%d", data.request.PrevLogTerm)).
				Str("leaderCommitIndex", fmt.Sprintf("%d", data.request.LeaderCommitIndex)).
				Msgf("Previous log not found")
		}
		return
	}

	if !data.request.Heartbeat {
		var newEntries []*raftypb.LogEntry
		for index, entry := range data.request.Entries {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
				Str("leaderAddress", data.request.LeaderAddress).
				Str("leaderId", data.request.LeaderID).
				Str("leaderCommitIndex", fmt.Sprintf("%d", data.request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", data.request.PrevLogIndex)).
				Str("leaderNewTotalLogs", fmt.Sprintf("%d", len(data.request.Entries))).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("index", fmt.Sprintf("%d", index)).
				Str("entryIndex", fmt.Sprintf("%d", entry.Index)).
				Msgf("debug data received append entries")

			if entry.Index > lastLogIndex || totalLogs == 0 {
				newEntries = data.request.Entries[index:]
				break
			}

			if totalLogs > 0 {
				lastLog := r.logs.fromIndex(entry.Index)
				if entry.Term != lastLog.logs[0].Term {
					wipe := r.logs.wipeEntries(entry.Index, lastLogIndex)
					if wipe.err != nil {
						r.Logger.Error().Err(wipe.err).
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("state", r.getState().String()).
							Str("term", fmt.Sprintf("%d", currentTerm)).
							Str("rangeFrom", fmt.Sprintf("%d", entry.Index)).
							Str("rangeTo", fmt.Sprintf("%d", lastLogIndex)).
							Str("leaderAddress", data.request.LeaderAddress).
							Str("leaderId", data.request.LeaderID).
							Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
							Msgf("Fail to remove conflicting log from range")
						data.responseChan <- response
						return
					}

					newEntries = data.request.Entries[index:]
					break
				}
			}
		}

		if lenEntries := len(newEntries); lenEntries > 0 {
			var (
				newPeers []peer
				err      error
			)
			totalLogs = r.logs.appendEntries(newEntries)
			peers, _ := r.getPeers()
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

				if newPeers, err = r.logs.applyConfigEntry(newEntries[index], peers); err != nil {
					r.Logger.Warn().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("leaderAddress", data.request.LeaderAddress).
						Str("leaderId", data.request.LeaderID).
						Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
						Msgf("Fail to apply config entry")
				}
				if newPeers != nil {
					r.lastAppliedConfig.Add(1)
				}

				if err := r.storage.data.store(newEntries[index]); err != nil {
					r.Logger.Fatal().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Msgf("Fail to persist data")
				}
			}

			if data.request.LeaderCommitIndex > r.commitIndex.Load() {
				r.commitIndex.Store(min(data.request.LeaderCommitIndex, uint64(totalLogs)))
			}
			r.lastApplied.Store(uint64(totalLogs - 1))

			r.Logger.Debug().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
				Str("leaderAddress", data.request.LeaderAddress).
				Str("leaderId", data.request.LeaderID).
				Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
				Str("leaderCommitIndex", fmt.Sprintf("%d", data.request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", data.request.PrevLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("lastLogIndex", fmt.Sprintf("%d", r.lastLogIndex.Load())).
				Str("matchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
				Str("lastApplied", fmt.Sprintf("%d", r.lastApplied.Load())).
				Str("lastAppliedConfig", fmt.Sprintf("%d", r.lastAppliedConfig.Load())).
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
	response.Term = data.request.Term
	response.Success = true
	data.responseChan <- response

	// this is only temporary as we added more debug logs
	if !r.options.ReadOnlyNode {
		r.switchState(Follower, stepDown, false, data.request.Term)
	}

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", data.request.Term)).
		Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
		Str("heartbeat", fmt.Sprintf("%t", data.request.Heartbeat)).
		Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
		Str("lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
		Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
		Str("leaderAddress", data.request.LeaderAddress).
		Str("leaderId", data.request.LeaderID).
		Str("leaderTerm", fmt.Sprintf("%d", data.request.Term)).
		Str("leaderPrevLogIndex", fmt.Sprintf("%d", data.request.PrevLogIndex)).
		Str("leaderPrevLogTerm", fmt.Sprintf("%d", data.request.PrevLogTerm)).
		Str("leaderCommitIndex", fmt.Sprintf("%d", data.request.LeaderCommitIndex)).
		Msgf("Received append entries from leader")
}
