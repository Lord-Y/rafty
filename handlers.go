package rafty

import (
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc/status"
)

func (r *Rafty) handleSendPreVoteRequestReader() {
	state := r.getState()
	currentTerm := r.getCurrentTerm()
	r.rpcPreVoteRequestChanWritter <- &raftypb.PreVoteResponse{
		PeerID:      r.ID,
		State:       state.String(),
		CurrentTerm: currentTerm,
	}
}

func (r *Rafty) handlePreVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil {
		r.Logger.Error().Err(vote.err).Msgf("Me %s / %s failed to get pre vote request from peer %s", r.Address.String(), r.ID, vote.peer.address.String())
	}
}

func (r *Rafty) handlePreVoteResponse(vote preVoteResponseWrapper) {
	currentTerm := r.getCurrentTerm()
	if vote.response.GetCurrentTerm() > currentTerm {
		r.Logger.Info().Msgf("Me %s / %s reports that peer %s / %s has a higher term than me %d > %d during pre vote", r.Address.String(), r.ID, vote.peer.address.String(), vote.response.GetPeerID(), vote.response.GetCurrentTerm(), currentTerm)
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		return
	}

	if vote.response.GetCurrentTerm() <= currentTerm {
		if !r.checkIfPeerInSliceIndex(true, vote.peer.address.String()) {
			r.appendPrecandidate(vote.peer)
			r.Logger.Info().Msgf("Me %s / %s reports that peer %s / %s will be part of the election campain", r.Address.String(), r.ID, vote.peer.address.String(), vote.response.GetPeerID())
		}

		var majority bool
		minimumClusterSize := r.getMinimumClusterSize()
		if r.leaderLost.Load() {
			if len(r.getPrecandidate())+1 == int(minimumClusterSize)-1 {
				r.Logger.Trace().Msgf("Me %s / %s reports PreCandidatePeers majority length when leader is lost %d", r.Address.String(), r.ID, r.MinimumClusterSize-1)
				majority = true
			}
		} else {
			// TODO: review this part when more than 3 nodes are involved
			if len(r.getPrecandidate())+1 == int(minimumClusterSize) {
				r.Logger.Trace().Msgf("Me %s / %s reports peer PreCandidatePeers majority length %d", r.Address.String(), r.ID, minimumClusterSize)
				majority = true
			}
		}

		if majority {
			r.Logger.Trace().Msgf("Me %s / %s reports that pre vote quorum as been reach", r.Address.String(), r.ID)
			r.startElectionCampain.Store(true)
			r.switchState(Candidate, false, currentTerm)
			r.stopElectionTimer()
		}
	}
}

func (r *Rafty) handleSendVoteRequestReader(reader *raftypb.VoteRequest) {
	state := r.getState()
	myAddress, myId := r.getMyAddress()
	currentTerm := r.getCurrentTerm()
	votedFor, _ := r.getVotedFor()
	lastLogIndex := r.getLastLogIndex()
	totalLogs := r.getTotalLogs()

	if currentTerm > reader.GetCurrentTerm() {
		r.Logger.Trace().Msgf("Me %s / %s with state %s rejected vote request from peer %s / %s, my current term %d > %d", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), currentTerm, reader.GetCurrentTerm())
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm:       currentTerm,
			PeerID:            r.ID,
			RequesterStepDown: true,
		}
		return
	}

	// if my current term is lower than candidate current term
	// set my current term to the candidate term
	// vote for the candidate
	if currentTerm < reader.GetCurrentTerm() {
		r.Logger.Trace().Msgf("Me %s / %s with state %s granted vote to peer %s / %s for term %d", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), reader.GetCurrentTerm())
		r.setCurrentTerm(reader.GetCurrentTerm())
		r.setVotedFor(reader.GetCandidateId(), reader.GetCurrentTerm())
		r.setLeaderLastContactDate()
		go r.switchState(Follower, false, reader.GetCurrentTerm())
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm: reader.GetCurrentTerm(),
			VoteGranted: true,
			PeerID:      r.ID,
		}
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
		}
		return
	}

	if votedFor != "" && votedFor != reader.GetCandidateId() {
		r.Logger.Trace().Msgf("Me %s / %s with state %s rejected vote request from peer %s / %s, for term %d because I already voted", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), currentTerm)
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm: currentTerm,
			PeerID:      r.ID,
		}
		return
	}

	if totalLogs > 0 {
		lastLogTerm := r.getLastLogTerm(r.log[r.lastLogIndex].Term)
		if reader.GetLastLogTerm() > lastLogTerm || (lastLogTerm == reader.GetLastLogTerm() && reader.GetLastLogIndex() >= lastLogIndex) {
			r.Logger.Trace().Msgf("Me %s / %s with state %s granted vote to peer %s / %s for term %d", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), reader.GetCurrentTerm())
			r.setVotedFor(reader.GetCandidateId(), reader.GetCurrentTerm())
			r.setLeaderLastContactDate()
			go r.switchState(Follower, false, reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
				CurrentTerm: currentTerm,
				PeerID:      r.ID,
				VoteGranted: true,
			}
			if err := r.persistMetadata(); err != nil {
				r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
			}
			return
		}

		r.Logger.Trace().Msgf("Me %s / %s with state %s rejected vote request from peer %s / %s for current term %d because my lastLogIndex %d > %d", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), currentTerm, lastLogIndex, reader.GetLastLogIndex())
		r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
			CurrentTerm:       currentTerm,
			PeerID:            r.ID,
			RequesterStepDown: true,
		}
		return
	}

	r.Logger.Trace().Msgf("Me %s / %s with state %s granted vote to peer %s / %s for term %d", myAddress, myId, state.String(), reader.GetCandidateAddress(), reader.GetCandidateId(), reader.GetCurrentTerm())
	r.setVotedFor(reader.GetCandidateId(), reader.GetCurrentTerm())
	r.setLeaderLastContactDate()
	go r.switchState(Follower, false, reader.GetCurrentTerm())
	r.rpcSendVoteRequestChanWritter <- &raftypb.VoteResponse{
		CurrentTerm: currentTerm,
		PeerID:      r.ID,
		VoteGranted: true,
	}
	if err := r.persistMetadata(); err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
	}
}

func (r *Rafty) handleVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil {
		r.Logger.Error().Err(vote.err).Msgf("Fail to send vote request to peer %s / %s with status code %s", vote.peer.address.String(), vote.peer.id, status.Code(vote.err))
	}
}

func (r *Rafty) handleVoteResponse(vote voteResponseWrapper) {
	if vote.savedCurrentTerm < vote.response.GetCurrentTerm() {
		r.Logger.Info().Msgf("Peer %s / %s has a higher term than me %d > %d", vote.peer.address.String(), vote.response.GetPeerID(), vote.response.GetCurrentTerm(), r.CurrentTerm)
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
		}
		return
	}

	if vote.response.GetNewLeaderDetected() {
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
		}
		return
	}

	if vote.response.GetVoteGranted() {
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
		r.Logger.Info().Msgf("Peer %s / %s already voted for someone", vote.peer.address.String(), vote.peer.id)
	}

	if vote.response.GetRequesterStepDown() {
		r.setCurrentTerm(vote.response.GetCurrentTerm())
		r.switchState(Follower, false, vote.response.GetCurrentTerm())
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
		}
		return
	}

	votes := 1 // voting for myself
	r.mu.Lock()
	quoroms := r.quoroms
	totalPrecandidates := len(r.PreCandidatePeers) + 1
	r.mu.Unlock()
	for _, q := range quoroms {
		if q.VoteGranted {
			votes += 1
		}
	}
	totalVotes := votes * 2
	if totalVotes > totalPrecandidates && r.getState() == Candidate {
		currentTerm := r.getCurrentTerm()
		r.Logger.Info().Msgf("Me %s / %s with term %d has won the election %d > %d ", r.Address.String(), r.ID, currentTerm, totalVotes, totalPrecandidates)
		r.switchState(Leader, false, currentTerm)
	}
}

func (r *Rafty) handleGetLeaderReader(reader *raftypb.GetLeaderRequest) {
	r.Logger.Trace().Msgf("Peer %s / %s is looking for the leader", reader.GetPeerAddress(), reader.GetPeerID())

	if r.getState() == Leader {
		go r.connectToPeer(reader.GetPeerAddress())
		r.rpcGetLeaderChanWritter <- &raftypb.GetLeaderResponse{
			LeaderID:      r.ID,
			LeaderAddress: r.Address.String(),
			PeerID:        r.ID,
		}
		return
	}

	leader := r.getLeader()
	if leader == nil {
		r.rpcGetLeaderChanWritter <- &raftypb.GetLeaderResponse{
			LeaderID:      "",
			LeaderAddress: "",
			PeerID:        r.ID,
		}
		return
	}

	r.rpcGetLeaderChanWritter <- &raftypb.GetLeaderResponse{
		LeaderID:      leader.id,
		LeaderAddress: leader.address,
		PeerID:        r.ID,
	}
}

func (r *Rafty) handleClientGetLeaderReader() {
	if r.getState() == Leader {
		r.rpcClientGetLeaderChanWritter <- &raftypb.ClientGetLeaderResponse{
			LeaderID:      r.ID,
			LeaderAddress: r.Address.String(),
		}
		return
	}

	if r.leader == nil {
		r.rpcClientGetLeaderChanWritter <- &raftypb.ClientGetLeaderResponse{
			LeaderID:      "",
			LeaderAddress: "",
		}
		return
	}

	r.rpcClientGetLeaderChanWritter <- &raftypb.ClientGetLeaderResponse{
		LeaderID:      r.leader.id,
		LeaderAddress: r.leader.address,
	}
}

func (r *Rafty) handleSendAppendEntriesRequestReader(reader *raftypb.AppendEntryRequest) {
	// if our local term is greater than leader term
	// reply false §5.1
	state := r.getState()
	myAddress, myId := r.getMyAddress()

	if !r.ReadOnlyNode {
		currentTerm := r.getCurrentTerm()
		if currentTerm > reader.GetTerm() {
			r.Logger.Error().Err(errTermTooOld).Msgf("Me %s / %s with state %s has higher term %d > %d than leader %s / %s for append entries", myAddress, myId, state.String(), currentTerm, reader.GetTerm(), reader.GetLeaderAddress(), reader.GetLeaderID())
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{
				Term: currentTerm,
			}
			return
		}

		switch state {
		case Follower:
			r.setCurrentTerm(reader.GetTerm())
		case Candidate:
			r.setCurrentTerm(reader.GetTerm())
			r.switchState(Follower, false, reader.GetTerm())
		case Leader:
			r.Logger.Error().Err(errAppendEntriesToLeader).Msgf("Me %s / %s with state %s cannot receive append entries from %s / %s for term %d", myAddress, myId, state.String(), reader.GetLeaderAddress(), reader.GetLeaderID(), currentTerm)
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{
				Term: currentTerm,
			}
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
			r.Logger.Error().Err(errTermTooOld).Msgf("Me %s / %s with state %s has no matching log from leader %s / %s append entries", myAddress, myId, state.String(), reader.GetLeaderAddress(), reader.GetLeaderID())
			r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{}
			return
		}

		nextIndex := reader.GetPrevLogIndex() + 1
		newEntries := 0
		for index := nextIndex; index < nextIndex+totalLogs; index++ {
			previousEntry := reader.GetEntries()[index-nextIndex]
			if index > uint64(cap(r.log)) {
				totalOfEntries := nextIndex + totalLogs
				newLog := make([]*raftypb.LogEntry, index, totalOfEntries*2)
				copy(newLog, r.log)
				r.log = newLog
			}

			if index < totalLogs && r.log[index].Term != previousEntry.Term {
				// conflicting entries in follower logs will be overwritten with entries from the leader's log
				r.log = r.log[:index]
				r.Logger.Debug().Msgf("Me %s / %s with state %s removed log entry %d from my logs due to conflicts with leader %s / %s logs", myAddress, myId, state.String(), index, reader.GetLeaderAddress(), reader.GetLeaderID())
			}

			if index > totalLogs {
				r.log = append(r.log, previousEntry)
				newEntries++
			}
		}
		if reader.GetLeaderCommitIndex() > r.commitIndex {
			r.commitIndex = min(reader.GetLeaderCommitIndex(), totalLogs-1)
		}
	}

	r.Logger.Trace().Msgf("Me %s / %s with state %s received heartbeat from leader %s / %s for term %d", myAddress, myId, state.String(), reader.GetLeaderAddress(), reader.GetLeaderID(), reader.GetTerm())
	leader := r.getLeader()
	if leader != nil && leader.id != reader.GetLeaderID() {
		r.saveLeaderInformations(leaderMap{
			id:      reader.GetLeaderID(),
			address: reader.GetLeaderAddress(),
		})
	}

	if leader == nil {
		r.saveLeaderInformations(leaderMap{
			id:      reader.GetLeaderID(),
			address: reader.GetLeaderAddress(),
		})
		r.switchState(Follower, false, reader.GetTerm())
	}

	r.mu.Lock()
	lastContactDate := time.Now()
	r.LeaderLastContactDate = &lastContactDate
	r.leaderLost.Store(false)
	r.mu.Unlock()

	r.rpcSendAppendEntriesRequestChanWritter <- &raftypb.AppendEntryResponse{Term: reader.GetTerm(), Success: true}
}
