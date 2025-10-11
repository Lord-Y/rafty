package rafty

import (
	"fmt"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// handleSendPreVoteRequest allow the current node to manage
// pre vote requests coming from other nodes
func (r *Rafty) handleSendPreVoteRequest(data RPCRequest) {
	r.wg.Add(1)
	defer r.wg.Done()
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
	r.wg.Add(1)
	defer r.wg.Done()
	request := data.Request.(*raftypb.VoteRequest)
	currentTerm := r.currentTerm.Load()
	votedFor, votedForTerm := r.getVotedFor()
	lastLogIndex := r.lastLogIndex.Load()
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

		response.CurrentTerm = request.CurrentTerm
		response.Granted = true
		data.ResponseChan <- rpcResponse
		r.switchState(Follower, stepDown, true, request.CurrentTerm)
		if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
			panic(err)
		}

		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Vote granted to peer")
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

	if votedFor != request.CandidateId && votedForTerm == request.CurrentTerm {
		response.CurrentTerm = currentTerm
		data.ResponseChan <- rpcResponse

		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", request.CandidateAddress).
			Str("peerId", request.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Vote request rejected to peer because I already voted")
		return
	}

	if r.lastLogIndex.Load() > 0 {
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("lastLogIndex", fmt.Sprintf("%d", r.lastLogIndex.Load())).
			Msgf("debug data lastLogTerm")

		if request.LastLogTerm > r.lastLogTerm.Load() || (r.lastLogTerm.Load() == request.LastLogTerm && request.LastLogIndex >= lastLogIndex) {
			r.setVotedFor(request.CandidateId, request.CurrentTerm)
			response.CurrentTerm = request.CurrentTerm
			response.Granted = true
			r.switchState(Follower, stepDown, false, request.CurrentTerm)
			if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
				panic(err)
			}
			data.ResponseChan <- rpcResponse

			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", request.CandidateAddress).
				Str("peerId", request.CandidateId).
				Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
				Msgf("Vote granted to peer")
			return
		}

		response.CurrentTerm = currentTerm
		data.ResponseChan <- rpcResponse

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
		return
	}

	r.setVotedFor(request.CandidateId, request.CurrentTerm)
	r.switchState(Follower, stepDown, false, request.CurrentTerm)
	if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
		panic(err)
	}

	response.CurrentTerm = request.CurrentTerm
	response.Granted = true
	data.ResponseChan <- rpcResponse

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", currentTerm)).
		Str("peerAddress", request.CandidateAddress).
		Str("peerId", request.CandidateId).
		Str("peerTerm", fmt.Sprintf("%d", request.CurrentTerm)).
		Msgf("Vote granted to peer")
}

// handleSendAppendEntriesRequest allow the current node to manage
// vote requests coming from other nodes
func (r *Rafty) handleSendAppendEntriesRequest(data RPCRequest) {
	// if our local term is greater than leader term
	// reply false §5.1
	r.wg.Add(1)
	defer r.wg.Done()
	request := data.Request.(*raftypb.AppendEntryRequest)
	currentTerm := r.currentTerm.Load()
	lastLogIndex := r.lastLogIndex.Load()
	lastLogTerm := r.lastLogTerm.Load()
	commitIndex := r.commitIndex.Load()
	response := &raftypb.AppendEntryResponse{
		Term:         currentTerm,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rpcResponse := RPCResponse{
		Response: response,
	}

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

	r.switchState(Follower, stepDown, true, request.Term)
	r.currentTerm.Store(request.Term)
	if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
		panic(err)
	}

	r.setLeader(leaderMap{
		id:      request.LeaderId,
		address: request.LeaderAddress,
	})
	r.leaderLastContactDate.Store(time.Now())
	r.timer.Reset(r.heartbeatTimeout())

	previousLogIndex, previousLogTerm := r.getPreviousLogIndexAndTerm()
	if (request.PrevLogIndex != previousLogIndex || request.PrevLogTerm != previousLogTerm) && !request.Catchup {
		response.LogNotFound = true
		response.LastLogIndex = lastLogIndex
		response.LastLogTerm = lastLogTerm
		data.ResponseChan <- rpcResponse

		if r.getState() != Down {
			r.Logger.Warn().Err(ErrLogNotFound).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("previousLogIndex", fmt.Sprintf("%d", previousLogIndex)).
				Str("previousLogTerm", fmt.Sprintf("%d", previousLogTerm)).
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

	if request.Catchup {
		var newEntries []*raftypb.LogEntry
		for index, entry := range request.Entries {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", request.Term)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
				Str("leaderPrevLogTerm", fmt.Sprintf("%d", request.PrevLogTerm)).
				Str("leaderNewTotalLogs", fmt.Sprintf("%d", len(request.Entries))).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", commitIndex)).
				Str("loopIndex", fmt.Sprintf("%d", index)).
				Str("entryIndex", fmt.Sprintf("%d", entry.Index)).
				Str("lastApplied", fmt.Sprintf("%d", r.lastApplied.Load())).
				Msgf("debug data received append entries")

			if entry.Index > lastLogIndex {
				newEntries = request.Entries[index:]
				break
			}

			if lastLogIndex > 0 && entry.Term != lastLogTerm {
				if err := r.logStore.DiscardLogs(entry.Index, lastLogIndex); err != nil {
					data.ResponseChan <- rpcResponse
					r.Logger.Error().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", request.Term)).
						Str("rangeFrom", fmt.Sprintf("%d", entry.Index)).
						Str("rangeTo", fmt.Sprintf("%d", lastLogIndex)).
						Str("leaderAddress", request.LeaderAddress).
						Str("leaderId", request.LeaderId).
						Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
						Msgf("Fail to remove conflicting log from range")
					return
				}

				newEntries = request.Entries[index:]
				break
			}
		}

		if lenEntries := len(newEntries); lenEntries > 0 {
			if err := r.logStore.StoreLogs(makeLogEntries(newEntries)); err != nil {
				panic(err)
			}

			lastLogIndex = newEntries[lenEntries-1].Index
			lastLogTerm = newEntries[lenEntries-1].Term
			r.lastLogIndex.Store(lastLogIndex)
			r.lastLogTerm.Store(lastLogTerm)
			for index := range newEntries {
				if err := r.applyConfigEntry(newEntries[index]); err != nil {
					r.Logger.Warn().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", request.Term)).
						Str("leaderAddress", request.LeaderAddress).
						Str("leaderId", request.LeaderId).
						Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
						Msgf("Fail to apply config entry")
				}
			}
			if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
				panic(err)
			}

			if _, err := r.applyLogs(applyLogs{entries: newEntries}); err != nil {
				r.Logger.Error().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("term", fmt.Sprintf("%d", request.Term)).
					Str("leaderAddress", request.LeaderAddress).
					Str("leaderId", request.LeaderId).
					Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
					Msgf("Fail to apply log entries to the fsm")
			}

			if request.LeaderCommitIndex > commitIndex {
				r.commitIndex.Store(min(request.LeaderCommitIndex, lastLogIndex))
			}

			if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
				panic(err)
			}

			r.Logger.Debug().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", request.Term)).
				Str("leaderAddress", request.LeaderAddress).
				Str("leaderId", request.LeaderId).
				Str("leaderTerm", fmt.Sprintf("%d", request.Term)).
				Str("leaderCommitIndex", fmt.Sprintf("%d", request.LeaderCommitIndex)).
				Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.PrevLogIndex)).
				Str("commitIndex", fmt.Sprintf("%d", r.commitIndex.Load())).
				Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
				Str("lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
				Str("lastApplied", fmt.Sprintf("%d", r.lastApplied.Load())).
				Str("lastAppliedConfigIndex", fmt.Sprintf("%d", r.lastAppliedConfigIndex.Load())).
				Str("lastAppliedConfigTerm", fmt.Sprintf("%d", r.lastAppliedConfigTerm.Load())).
				Msgf("Node state index updated")
		}
	}

	response.LastLogIndex = lastLogIndex
	response.LastLogTerm = lastLogTerm
	response.Term = request.Term
	response.Success = true
	data.ResponseChan <- rpcResponse

	if r.shutdownOnRemove.Load() {
		go r.stop()
	}
}

func (r *Rafty) handleInstallSnapshotRequest(data RPCRequest) {
	r.wg.Add(1)
	defer r.wg.Done()
	defer r.metrics.timeSince("installSnapshot", time.Now())
	request := data.Request.(*raftypb.InstallSnapshotRequest)
	currentTerm := r.currentTerm.Load()
	response := &raftypb.InstallSnapshotResponse{
		Term: currentTerm,
	}
	rpcResponse := RPCResponse{
		Response: response,
	}

	defer func() {
		data.ResponseChan <- rpcResponse
	}()

	if currentTerm > request.CurrentTerm {
		rpcResponse.Error = ErrTermTooOld
		return
	}

	if request.CurrentTerm > currentTerm {
		r.currentTerm.Store(request.CurrentTerm)
		response.Term = request.CurrentTerm
		r.switchState(Follower, stepDown, true, request.CurrentTerm)
	}

	r.setLeader(leaderMap{
		id:      request.LeaderId,
		address: request.LeaderAddress,
	})
	r.leaderLastContactDate.Store(time.Now())

	peers, _ := DecodePeers(request.Configuration)
	configuration := Configuration{
		ServerMembers: peers,
	}
	snapshot, err := r.snapshot.PrepareSnapshotWriter(request.LastIncludedIndex, request.LastIncludedTerm, request.LastAppliedConfigIndex, request.LastAppliedConfigTerm, configuration)
	if err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Msgf("Fail to preprare snapshot config")
		return
	}

	size, err := snapshot.Write(request.Data)
	if err != nil {
		_ = snapshot.Discard()
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Fail to write snapshot")
		return
	}

	if size != int(request.Size) {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Snapshot size invalid")
		return
	}

	if err := snapshot.Close(); err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Fail to close snapshot")
	}

	_, file, err := r.snapshot.PrepareSnapshotReader(snapshot.Name())
	if err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Fail to read snapshot")
	}
	defer func() {
		_ = file.Close()
	}()

	if err := r.fsm.Restore(file); err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Fail to restore snapshot")
	}

	response.Term = request.CurrentTerm
	response.Success = true
	r.lastLogIndex.Store(request.LastIncludedIndex)
	r.lastLogTerm.Store(request.LastIncludedTerm)
	r.lastApplied.Store(r.lastLogIndex.Load())
	r.commitIndex.Store(r.lastLogIndex.Load())

	if err := r.applyConfigEntry(&raftypb.LogEntry{
		LogType: uint32(LogConfiguration),
		Command: request.Configuration,
		Index:   request.LastAppliedConfigIndex,
		Term:    request.LastAppliedConfigTerm,
	}); err != nil {
		r.Logger.Warn().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Msgf("Fail to apply config entry")
	}

	if err := r.logStore.CompactLogs(request.LastIncludedIndex); err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("leaderAddress", request.LeaderAddress).
			Str("leaderId", request.LeaderId).
			Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
			Str("snapshotName", snapshot.Name()).
			Str("leaderLastIncludedIndex", fmt.Sprintf("%d", request.LastIncludedIndex)).
			Str("leaderLastIncludedTerm", fmt.Sprintf("%d", request.LastIncludedTerm)).
			Msgf("Fail to compact logs")
	}

	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", request.CurrentTerm)).
		Str("leaderAddress", request.LeaderAddress).
		Str("leaderId", request.LeaderId).
		Str("leaderTerm", fmt.Sprintf("%d", request.CurrentTerm)).
		Str("snapshotName", snapshot.Name()).
		Msgf("Restore snapshot successful")
}
