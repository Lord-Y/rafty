package rafty

import (
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/status"
)

func (r *Rafty) handleSendPreVoteRequestReader() {
	r.rpcPreVoteRequestChanWritter <- &grpcrequests.PreVoteResponse{
		PeerID:      r.ID,
		State:       r.State.String(),
		CurrentTerm: r.CurrentTerm,
	}
}

func (r *Rafty) handlePreVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil {
		r.Logger.Error().Err(vote.err).Msgf("Fail to get pre request vote from peer %s", vote.peer.address.String())
	}
}

func (r *Rafty) handlePreVoteResponse(vote preVoteResponseWrapper) {
	if vote.response.GetState() == Follower.String() || vote.response.GetState() == Candidate.String() {
		if vote.response.GetCurrentTerm() > r.CurrentTerm {
			r.Logger.Info().Msgf("Peer %s / %s has a higher term %d > %d", vote.peer.address.String(), vote.response.GetPeerID(), vote.response.GetCurrentTerm(), r.CurrentTerm)
			r.stopElectionTimer(true)
			r.CurrentTerm = vote.response.GetCurrentTerm()
			r.switchState(Follower, true)
			return
		}

		if vote.response.GetCurrentTerm() <= r.CurrentTerm {
			if !r.checkIfPeerInSliceIndex(true, vote.peer.address.String()) {
				r.PreCandidatePeers = append(r.PreCandidatePeers, vote.peer)
				log.Info().Msgf("Peer %s / %s will be part of the election campain", vote.peer.address.String(), vote.response.GetPeerID())
			}

			var majority bool
			switch {
			case len(r.Peers)+1 == 3:
				if len(r.PreCandidatePeers)+1 >= 2 {
					majority = true
				}
			case len(r.Peers)+1 == 5:
				if len(r.PreCandidatePeers)+1 >= 3 {
					majority = true
				}
			}

			if majority {
				r.stopElectionTimer(true)
				r.startElectionCampain = true
				log.Info().Msg("Pre vote quorum as been reach, let's start election campain")
				return
			}
			return
		}
	}
	r.switchState(Follower, true)
}

func (r *Rafty) handleSendVoteRequestReader(reader *grpcrequests.VoteRequest) {
	// if the requester is a candidate and I am a follower
	if reader.GetState() == Candidate.String() && (r.State == Candidate || r.State == Follower) {
		// if I already vote for someone for the current term
		// reject the vote request
		if r.CurrentTerm == reader.GetCurrentTerm() {
			r.Logger.Trace().Msgf("Reject vote request from peer %s for current term %d because I already voted", reader.GetId(), r.CurrentTerm)
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				AlreadyVoted:       true,
			}
			return
		}

		// if my current term is greater than candidate current term
		// reject the vote request
		if r.CurrentTerm > reader.GetCurrentTerm() {
			r.Logger.Trace().Msgf("Reject vote request from peer %s, my current term %d > %d", reader.GetId(), r.CurrentTerm, reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				RequesterStepDown:  true,
			}
			return
		}

		// if my current term is lower than candidate current term
		// set my current term to the candidate term
		// vote for the candidate
		if r.CurrentTerm < reader.GetCurrentTerm() {
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d", reader.GetId(), reader.GetCurrentTerm())
			r.CurrentTerm = reader.GetCurrentTerm()
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}

		// if my current term is equal to candidate current term and
		// if my current commit index is greater than candidate current commit index
		// reject the vote request
		if r.CurrentCommitIndex > reader.GetCurrentCommitIndex() {
			r.Logger.Trace().Msgf("Reject vote request from peer %s, my current commit index %d > %d", reader.GetId(), r.CurrentCommitIndex, reader.GetCurrentCommitIndex())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				RequesterStepDown:  true,
			}
			return
		}

		// if my current commit index is lower than candidate current commit index
		// vote for the candidate
		if r.CurrentCommitIndex < reader.GetCurrentCommitIndex() {
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d, my current commit index %d < %d", reader.GetId(), reader.GetCurrentTerm(), r.CurrentCommitIndex, reader.GetCurrentCommitIndex())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}

		// if my current commit index is equal to candidate current commit index and
		// if my last applied index is greater to candidate last applied index
		// reject the vote request
		if r.LastApplied > reader.GetLastApplied() {
			r.Logger.Trace().Msgf("Reject vote request from peer %s, my last applied index %d > %d", reader.GetId(), r.LastApplied, reader.GetLastApplied())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				RequesterStepDown:  true,
			}
			return
		}

		// if my last applied index is lower or equal to candidate last applied index
		// vote for the candidate
		if r.LastApplied <= reader.GetLastApplied() {
			r.CurrentTerm = reader.GetCurrentTerm()
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d", reader.GetId(), reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}
	}

	// if I'm the current leader but the requester is not aware of it
	if r.State == Leader {
		// As the new leader if my current term is greater than the candidate current term
		// reject the vote request
		if r.CurrentTerm > reader.GetCurrentTerm() {
			r.Logger.Trace().Msgf("I'm the new leader, reject vote request from peer %s, my current term %d > %d", reader.GetId(), r.CurrentTerm, reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        false,
				RequesterStepDown:  true,
			}
			return
		}

		// As the new leader if my current term is lower than the candidate current term
		// set my current term to the candidate term
		// vote for the candidate
		// step down as follower
		if r.CurrentTerm < reader.GetCurrentTerm() {
			r.CurrentTerm = reader.GetCurrentTerm()
			r.saveLeaderInformations(leaderMap{})
			r.switchState(Follower, true)
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d", reader.GetId(), reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}

		// As the new leader if my current term equal to the candidate current term and
		// if my current commit index greater than the candidate current commit index
		// reject his vote
		// tell him who is the leader and ask him to step down
		if r.CurrentCommitIndex > reader.GetCurrentCommitIndex() {
			r.Logger.Trace().Msgf("I'm the new leader, reject vote request from peer %s, my current commit index %d > %d", reader.GetId(), r.CurrentCommitIndex, reader.GetCurrentCommitIndex())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}
			return
		}

		// As the new leader if my current commit index lower than the candidate current commit index
		// vote for the candidate
		// step down as follower
		if r.CurrentCommitIndex < reader.GetCurrentCommitIndex() {
			r.saveLeaderInformations(leaderMap{})
			r.switchState(Follower, false)
			r.Logger.Trace().Msgf("Stepping down as follower, my current commit index %d < %d", r.CurrentCommitIndex, reader.GetCurrentCommitIndex())
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d", reader.GetId(), reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}

		// As the new leader if my current commit index equal to the candidate current commit index and
		// if my last applied index greater than the candidate last applied index
		// reject his vote
		// tell him who is the leader and ask him to step down
		if r.LastApplied > reader.GetLastApplied() {
			r.Logger.Trace().Msgf("I'm the new leader, reject vote request from peer %s, my last applied index %d > %d", reader.GetId(), r.LastApplied, reader.GetLastApplied())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}
			return
		}

		// As the new leader if my last applied index is lower than the candidate last applied index
		// vote for the candidate
		// step down as follower
		if r.LastApplied < reader.GetLastApplied() {
			r.saveLeaderInformations(leaderMap{})
			r.switchState(Follower, false)
			r.Logger.Trace().Msgf("Vote granted to peer %s for term %d", reader.GetId(), reader.GetCurrentTerm())
			r.Logger.Trace().Msgf("Stepping down as follower as my last applied index %d < %d", r.LastApplied, reader.GetLastApplied())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        true,
			}
			return
		}

		// As the new leader if my last applied index is greater than the candidate last applied index
		// reject his vote request
		// I'm sticking as the leader
		// tell him who is the him who is the leader and ask him to step down
		if r.LastApplied == reader.GetLastApplied() {
			r.Logger.Trace().Msgf("I'm the new leader, reject vote request from peer %s, my last applied index %d == %d for term %d", reader.GetId(), r.LastApplied, reader.GetLastApplied(), reader.GetCurrentTerm())
			r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
				CurrentTerm:        r.CurrentTerm,
				CurrentCommitIndex: r.CurrentCommitIndex,
				LastApplied:        r.LastApplied,
				PeerID:             r.ID,
				VoteGranted:        false,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}
			return
		}

		// reject anyway
		// step down as follower
		r.switchState(Follower, true)
		r.rpcSendVoteRequestChanWritter <- &grpcrequests.VoteResponse{
			CurrentTerm:        r.CurrentTerm,
			CurrentCommitIndex: r.CurrentCommitIndex,
			LastApplied:        r.LastApplied,
			PeerID:             r.ID,
			VoteGranted:        false,
		}
	}
}

func (r *Rafty) handleVoteResponseError(vote voteResponseErrorWrapper) {
	if vote.err != nil {
		r.Logger.Err(vote.err).Msgf("Fail to send vote request to peer %s / %s with status code %v", vote.peer.address.String(), vote.peer.id, status.Code(vote.err))
	}
}

func (r *Rafty) handleVoteResponse(vote voteResponseWrapper) {
	if r.State != Candidate {
		return
	}

	if vote.savedCurrentTerm < vote.response.GetCurrentTerm() {
		r.CurrentTerm = vote.response.GetCurrentTerm()
		r.resetElectionTimer(false)
		r.runAsFollower()
		return
	}
	if vote.response.GetNewLeaderDetected() {
		r.resetElectionTimer(true)
		r.CurrentTerm = vote.response.GetCurrentTerm()
		r.runAsFollower()
	}
	if vote.response.GetVoteGranted() {
		r.quoroms = append(r.quoroms, &quorom{
			VoterID:     vote.response.GetPeerID(),
			VoteGranted: true,
		})
	} else {
		r.quoroms = append(r.quoroms, &quorom{
			VoterID: vote.response.GetPeerID(),
		})

		if vote.response.GetAlreadyVoted() {
			r.Logger.Info().Msgf("Peer %s / %s already voted for someone", vote.peer.address.String(), vote.peer.id)
		}
	}
	if vote.response.GetRequesterStepDown() {
		r.CurrentTerm = vote.response.GetCurrentTerm()
		r.resetElectionTimer(false)
		r.runAsFollower()
		return
	}

	votes := 1 // voting for myself
	for _, q := range r.quoroms {
		if q.VoteGranted {
			votes += 1
		}
	}
	if votes*2 > len(r.PreCandidatePeers)+1 {
		r.stopElectionTimer(true)
		r.stopElectionTimer(false)
		r.Logger.Trace().Msgf("Me %s / %s with term %d has won the election", r.Address.String(), r.ID, r.CurrentTerm)

		r.runAsLeader()
		return
	}
}

func (r *Rafty) handleGetLeaderReader(reader *grpcrequests.GetLeaderRequest) {
	r.Logger.Trace().Msgf("Peer %s / %s is looking for the leader", reader.GetPeerAddress(), reader.GetPeerID())
	go r.switchPeerToUpOrDown(reader.GetPeerID(), reader.GetPeerAddress(), true)
	if r.leader == nil {
		r.rpcGetLeaderChanWritter <- &grpcrequests.GetLeaderResponse{
			LeaderID:      "",
			LeaderAddress: "",
			PeerID:        r.ID,
		}
		return
	}

	r.rpcGetLeaderChanWritter <- &grpcrequests.GetLeaderResponse{
		LeaderID:      r.leader.id,
		LeaderAddress: r.leader.address,
		PeerID:        r.ID,
	}
}

func (r *Rafty) handleSetLeaderReader(reader *grpcrequests.SetLeaderRequest) {
	r.saveLeaderInformations(leaderMap{
		id:      reader.GetId(),
		address: reader.GetAddress(),
	})
	r.switchState(Follower, true)
	lastContactDate := time.Now()
	r.LeaderLastContactDate = &lastContactDate
	r.resetElectionTimer(false)
	r.rpcSetLeaderChanWritter <- &grpcrequests.SetLeaderResponse{Message: reader.GetId()}
}

func (r *Rafty) handleSendHeartbeatsReader(reader *grpcrequests.SendHeartbeatRequest) {
	// if I am the current leader and I receive heartbeat from an another leader
	if r.State == Leader {
		r.Logger.Trace().Msgf("Multiple leaders detected for term %d", reader.GetCurrentTerm())
		r.rpcSendHeartbeatsChanWritter <- &grpcrequests.SendHeartbeatResponse{
			PeerID:          r.ID,
			CurrentTerm:     r.CurrentTerm,
			MultipleLeaders: true,
		}
		return
	}

	r.Logger.Trace().Msgf("Heartbeat received from leader %s / %s for term %d", reader.GetLeaderAddress(), reader.GetLeaderID(), reader.GetCurrentTerm())
	if r.leader != nil && r.leader.id != reader.GetLeaderID() && r.leader.address != reader.GetLeaderAddress() {
		r.saveLeaderInformations(leaderMap{
			id:      reader.GetLeaderID(),
			address: reader.GetLeaderAddress(),
		})
	}
	lastContactDate := time.Now()
	r.mu.Lock()
	r.LeaderLastContactDate = &lastContactDate
	r.mu.Unlock()
	r.stopElectionTimer(true)
	r.resetElectionTimer(false)

	if r.CurrentTerm < reader.GetCurrentTerm() {
		r.CurrentTerm = reader.GetCurrentTerm()
	}

	r.rpcSendHeartbeatsChanWritter <- &grpcrequests.SendHeartbeatResponse{
		PeerID:      r.ID,
		CurrentTerm: r.CurrentTerm,
	}
}

func (r *Rafty) handleHeartBeatsResponseError(response heartbeatErrorWrapper) {
	if response.err != nil {
		got := status.Code(response.err)
		r.Logger.Err(response.err).Msgf("Fail to request send heartbeat to peer %s / %s with status code %v", response.peer.address.String(), response.peer.id, got)
	}
}

func (r *Rafty) handleHeartBeatsResponse(response heartbeatResponseWrapper) {
	if response.response.GetMultipleLeaders() {
		r.heartbeatTicker.Stop()
		r.CurrentTerm = response.response.GetCurrentTerm()
		r.resetElectionTimer(false)
		r.saveLeaderInformations(leaderMap{})
		r.switchState(Follower, true)
	}

	if r.CurrentTerm < response.response.GetCurrentTerm() {
		r.heartbeatTicker.Stop()
		r.CurrentTerm = response.response.GetCurrentTerm()
		r.saveLeaderInformations(leaderMap{})
		r.resetElectionTimer(false)
		r.switchState(Follower, true)
	}
}
