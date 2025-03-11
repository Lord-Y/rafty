package rafty

import (
	"fmt"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc/status"
)

func (r *Rafty) handleSendPreVoteRequestReader() {
	state := r.getState()
	currentTerm := r.getCurrentTerm()
	r.rpcPreVoteRequestChanWritter <- &raftypb.PreVoteResponse{
		PeerID:      r.id,
		State:       state.String(),
		CurrentTerm: currentTerm,
	}
}

func (r *Rafty) handlePreVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil && r.getState() != Down {
		r.Logger.Error().Err(vote.err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("peerAddress", vote.peer.address.String()).
			Str("peerId", vote.peer.ID).
			Msgf("Fail to get pre vote request")
	}
}

func (r *Rafty) handlePreVoteResponse(vote preVoteResponseWrapper) {
	currentTerm := r.getCurrentTerm()
	if vote.response.GetCurrentTerm() > currentTerm {
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("peerAddress", vote.peer.address.String()).
			Str("peerId", vote.peer.ID).
			Msgf("Peer has higher term than me %d > %d", vote.response.CurrentTerm, currentTerm)
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		return
	}

	if vote.response.CurrentTerm <= currentTerm {
		if !r.checkIfPeerInSliceIndex(true, vote.peer.address.String()) {
			r.appendPrecandidate(vote.peer)
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("peerAddress", vote.peer.address.String()).
				Str("peerId", vote.peer.ID).
				Msgf("Peer will be part of the election campain")
		}

		var majority bool
		minimumClusterSize := r.getMinimumClusterSize()
		if r.leaderLost.Load() {
			if len(r.getPrecandidate())+1 == int(minimumClusterSize)-1 {
				majority = true
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("term", fmt.Sprintf("%d", currentTerm)).
					Msgf("PreCandidatePeers majority length when leader is lost is %d", r.options.MinimumClusterSize-1)
			}
		} else {
			// TODO: review this part when more than 3 nodes are involved
			if len(r.getPrecandidate())+1 == int(minimumClusterSize) {
				majority = true
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("term", fmt.Sprintf("%d", currentTerm)).
					Msgf("PreCandidatePeers majority length is %d", minimumClusterSize)
			}
		}

		if majority {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Msgf("Pre vote quorum as been reach")

			r.startElectionCampain.Store(true)
			r.switchState(Candidate, false, currentTerm)
			r.stopElectionTimer()
		}
	}
}

func (r *Rafty) handleSendVoteRequestReader(reader *raftypb.VoteRequest) {
	currentTerm := r.getCurrentTerm()
	votedFor, _ := r.getVotedFor()
	lastLogIndex := r.getLastLogIndex()
	totalLogs := r.getTotalLogs()

	if currentTerm > reader.CurrentTerm {
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm:       currentTerm,
			PeerID:            r.id,
			RequesterStepDown: true,
		}
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", reader.CandidateAddress).
			Str("peerId", reader.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", reader.CurrentTerm)).
			Msgf("Reject vote request from peer")
		return
	}

	// if my current term is lower than candidate current term
	// set my current term to the candidate term
	// vote for the candidate
	if currentTerm < reader.CurrentTerm {
		r.setCurrentTerm(reader.GetCurrentTerm())
		r.setVotedFor(reader.GetCandidateId(), reader.GetCurrentTerm())
		r.setLeaderLastContactDate()
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm: reader.GetCurrentTerm(),
			VoteGranted: true,
			PeerID:      r.id,
		}
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", reader.CandidateAddress).
			Str("peerId", reader.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", reader.CurrentTerm)).
			Msgf("Grant vote to peer")
		r.switchState(Follower, false, reader.GetCurrentTerm())
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Fail to persist metadata")
		}
		return
	}

	if votedFor != "" && votedFor != reader.CandidateId {
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm: currentTerm,
			PeerID:      r.id,
		}
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("peerAddress", reader.CandidateAddress).
			Str("peerId", reader.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", reader.CurrentTerm)).
			Msgf("Reject vote request from peer because I already voted")
		return
	}

	if totalLogs > 0 {
		lastLogTerm := r.getLastLogTerm(r.log[r.lastLogIndex].Term)
		if reader.GetLastLogTerm() > lastLogTerm || (lastLogTerm == reader.GetLastLogTerm() && reader.GetLastLogIndex() >= lastLogIndex) {
			r.setVotedFor(reader.GetCandidateId(), reader.GetCurrentTerm())
			r.setLeaderLastContactDate()
			r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
				CurrentTerm: currentTerm,
				PeerID:      r.id,
				VoteGranted: true,
			}
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", reader.CandidateAddress).
				Str("peerId", reader.CandidateId).
				Str("peerTerm", fmt.Sprintf("%d", reader.CurrentTerm)).
				Msgf("Grant vote to peer")
			r.switchState(Follower, false, reader.GetCurrentTerm())
			if err := r.persistMetadata(); err != nil {
				r.Logger.Fatal().Err(err).
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Fail to persist metadata")
			}
			return
		}

		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm:       currentTerm,
			PeerID:            r.id,
			RequesterStepDown: true,
		}
		r.Logger.Trace().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", currentTerm)).
			Str("lastLogIndex", fmt.Sprintf("%d", lastLogIndex)).
			Str("peerAddress", reader.CandidateAddress).
			Str("peerId", reader.CandidateId).
			Str("peerTerm", fmt.Sprintf("%d", reader.LastLogIndex)).
			Str("peerLastLogIndex", fmt.Sprintf("%d", reader.CurrentTerm)).
			Msgf("Reject vote request from peer because of lastLogIndex")
		return
	}

	r.setVotedFor(reader.CandidateId, reader.CurrentTerm)
	r.setLeaderLastContactDate()
	r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
		CurrentTerm: currentTerm,
		PeerID:      r.id,
		VoteGranted: true,
	}
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", currentTerm)).
		Str("peerAddress", reader.CandidateAddress).
		Str("peerId", reader.CandidateId).
		Str("peerTerm", fmt.Sprintf("%d", reader.CurrentTerm)).
		Msgf("Grant vote to peer")
	r.switchState(Follower, false, reader.CurrentTerm)
	if err := r.persistMetadata(); err != nil {
		r.Logger.Fatal().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Fail to persist metadata")
	}
}

func (r *Rafty) handleVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil && r.getState() != Down {
		r.Logger.Error().Err(vote.err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("peerAddress", vote.peer.address.String()).
			Str("peerId", vote.peer.ID).
			Str("statusCode", status.Code(vote.err).String()).
			Msgf("Fail to send vote request to peer")
	}
}

func (r *Rafty) handleVoteResponse(vote voteResponseWrapper) {
	if vote.savedCurrentTerm < vote.response.CurrentTerm {
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", vote.savedCurrentTerm)).
			Str("peerAddress", vote.peer.address.String()).
			Str("peerId", vote.peer.ID).
			Str("peerTerm", fmt.Sprintf("%d", vote.response.CurrentTerm)).
			Msgf("Peer has higher term than me")
		r.setCurrentTerm(vote.response.CurrentTerm)
		r.switchState(Follower, false, vote.response.CurrentTerm)
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Fail to persist metadata")
		}
		return
	}

	if vote.response.NewLeaderDetected {
		r.setCurrentTerm(vote.response.CurrentTerm)
		r.switchState(Follower, false, vote.response.CurrentTerm)
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Fail to persist metadata")
		}
		return
	}

	if vote.response.VoteGranted {
		r.mu.Lock()
		r.quoroms = append(r.quoroms, quorom{
			VoterID:     vote.response.GetPeerID(),
			VoteGranted: true,
		})
		r.mu.Unlock()
	} else {
		r.mu.Lock()
		r.quoroms = append(r.quoroms, quorom{
			VoterID: vote.response.GetPeerID(),
		})
		r.mu.Unlock()
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("peerAddress", vote.peer.address.String()).
			Str("peerId", vote.peer.ID).
			Msgf("Peer already voted for someone")
	}

	if vote.response.GetRequesterStepDown() {
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Msgf("Fail to persist metadata")
		}
		return
	}

	votes := 1 // voting for myself
	r.mu.Lock()
	quoroms := r.quoroms
	totalPrecandidates := len(r.configuration.preCandidatePeers) + 1
	r.mu.Unlock()
	for _, q := range quoroms {
		if q.VoteGranted {
			votes += 1
		}
	}
	totalVotes := votes * 2
	if totalVotes > totalPrecandidates && r.getState() == Candidate {
		currentTerm := r.getCurrentTerm()
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("term", fmt.Sprintf("%d", vote.savedCurrentTerm)).
			Msgf("I won the election %d > %d", totalVotes, totalPrecandidates)
		r.switchState(Leader, true, currentTerm)
	}
}

func (r *Rafty) handleGetLeaderReader(reader *raftypb.GetLeaderRequest) {
	if r.getState() == Leader {
		go r.connectToPeer(reader.GetPeerAddress())
		r.rpcGetLeaderChanWritter <- &raftypb.GetLeaderResponse{
			LeaderID:      r.id,
			LeaderAddress: r.Address.String(),
			PeerID:        r.id,
		}
		return
	}

	leader := r.getLeader()
	r.rpcGetLeaderChanWritter <- &raftypb.GetLeaderResponse{
		LeaderID:      leader.id,
		LeaderAddress: leader.address,
		PeerID:        r.id,
	}
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("peerAddress", reader.PeerAddress).
		Str("peerId", reader.PeerID).
		Msgf("Peer is looking for the leader")
}

func (r *Rafty) handleClientGetLeaderReader() {
	if r.getState() == Leader {
		r.rpcClientGetLeaderChanWritter <- &raftypb.ClientGetLeaderResponse{
			LeaderID:      r.id,
			LeaderAddress: r.Address.String(),
		}
		return
	}

	leader := r.getLeader()
	r.rpcClientGetLeaderChanWritter <- &raftypb.ClientGetLeaderResponse{
		LeaderID:      leader.id,
		LeaderAddress: leader.address,
	}
}

func (r *Rafty) handleSendAppendEntriesRequestReader(reader *raftypb.AppendEntryRequest) {
	// if our local term is greater than leader term
	// reply false §5.1
	state := r.getState()

	if !r.options.ReadOnlyNode {
		currentTerm := r.getCurrentTerm()
		if currentTerm > reader.Term {
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{
				Term: currentTerm,
			}
			r.Logger.Error().Err(errTermTooOld).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("peerAddress", reader.LeaderAddress).
				Str("peerId", reader.LeaderID).
				Str("peerTerm", fmt.Sprintf("%d", reader.Term)).
				Msgf("My term is higher than peer")
			return
		}

		switch state {
		case Follower:
			r.setCurrentTerm(reader.GetTerm())
		case Candidate:
			r.setCurrentTerm(reader.GetTerm())
			r.switchState(Follower, false, reader.GetTerm())
		case Leader:
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{
				Term: currentTerm,
			}
			r.Logger.Error().Err(errAppendEntriesToLeader).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("peerAddress", reader.LeaderAddress).
				Str("peerId", reader.LeaderID).
				Str("peerTerm", fmt.Sprintf("%d", reader.Term)).
				Msgf("Cannot receive append entries from peer")
			return
		}

		r.resetElectionTimer()
		r.stopElectionTimer()
	}

	totalLogs := uint64(len(r.log))
	if totalLogs > 0 && !reader.GetHeartbeat() {
		// reader.GetPrevLogIndex() == 0 is the induction step: the initial empty state of logs that satisfy the Log Matching Property
		matchingLog := reader.GetPrevLogIndex() == 0 || (reader.GetPrevLogIndex() < totalLogs && r.log[reader.PrevLogIndex].Term == reader.GetPrevLogTerm())

		// if we do not find any log that match previous log index
		// reply false
		if !matchingLog {
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{}
			r.Logger.Error().Err(errTermTooOld).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("peerAddress", reader.LeaderAddress).
				Str("peerId", reader.LeaderID).
				Str("peerTerm", fmt.Sprintf("%d", reader.Term)).
				Msgf("No log matching with the leader")
			return
		}

		nextIndex := reader.GetPrevLogIndex() + 1
		newEntries := 0
		for index := nextIndex; index < nextIndex+totalLogs; index++ {
			if index > uint64(cap(r.log)) {
				totalOfEntries := nextIndex + totalLogs
				newLog := make([]*raftypb.LogEntry, index, totalOfEntries*2)
				copy(newLog, r.log)
				r.log = newLog
			}

			previousEntry := reader.GetEntries()[index-nextIndex]
			if index < totalLogs && r.log[index].Term != previousEntry.Term {
				// conflicting entries in follower logs will be overwritten with entries from the leader's log
				r.log = r.log[:index]
				r.Logger.Debug().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("peerAddress", reader.LeaderAddress).
					Str("peerId", reader.LeaderID).
					Str("peerTerm", fmt.Sprintf("%d", reader.Term)).
					Msgf("Removed log entry %d from my logs due to conflicts with leader", index)
			}

			if index > totalLogs {
				r.log = append(r.log, previousEntry)
				newEntries++
			}
		}
		if reader.LeaderCommitIndex > r.commitIndex {
			r.commitIndex = min(reader.LeaderCommitIndex, totalLogs-1)
		}
	}

	r.setLeader(leaderMap{
		id:      reader.GetLeaderID(),
		address: reader.GetLeaderAddress(),
	})
	r.leaderLost.Store(false)
	r.switchState(Follower, false, reader.GetTerm())
	r.setLeaderLastContactDate()

	r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{Term: reader.GetTerm(), Success: true}

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", reader.Term)).
		Str("leaderAddress", reader.LeaderAddress).
		Str("leaderId", reader.LeaderID).
		Str("peerTerm", fmt.Sprintf("%d", reader.Term)).
		Msgf("Received heartbeat from leader")
}
