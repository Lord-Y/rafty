package rafty

import (
	"context"
	"log"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
)

func (g *ProtobufSVC) SayHello(ctx context.Context, in *grpcrequests.HelloRequest) (*grpcrequests.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &grpcrequests.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (g *ProtobufSVC) RequestVotes(ctx context.Context, in *grpcrequests.RequestVote) (*grpcrequests.RequestVoteReply, error) {
	g.rafty.mu.Lock()
	defer g.rafty.mu.Unlock()

	// if the requester is a candidate and I am a follower
	if in.GetState() == Candidate.String() && (g.rafty.State == Candidate || g.rafty.State == Follower) {
		// if I already vote for someone for the current term
		// reject the request vote
		if g.rafty.votedFor != "" && g.rafty.CurrentTerm == in.GetCurrentTerm() {
			g.Logger.Info().Msgf("reject request vote from peer %s for current term %d because I already voted", in.GetId(), g.rafty.CurrentTerm)
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				AlreadyVoted:       true,
			}, nil
		}

		// if my current term is greater than candidate current term
		// reject the request vote
		if g.rafty.CurrentTerm > in.GetCurrentTerm() {
			g.Logger.Info().Msgf("reject request vote from peer %s, my current term %d > %d", in.GetId(), g.rafty.CurrentTerm, in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				RequesterStepDown:  true,
			}, nil
		}

		// if my current term is lower than candidate current term
		// set my current term to the candidate term
		// vote for the candidate
		if g.rafty.CurrentTerm < in.GetCurrentTerm() {
			g.rafty.votedFor = in.GetId()
			g.Logger.Info().Msgf("vote granted to peer %s for term %d", in.GetId(), in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}

		// if my current term is equal to candidate current term and
		// if my current commit index is greater than candidate current commit index
		// reject the request vote
		if g.rafty.CurrentCommitIndex > in.GetCurrentCommitIndex() {
			g.Logger.Info().Msgf("reject request vote from peer %s, my current commit index %d > %d", in.GetId(), g.rafty.CurrentCommitIndex, in.GetCurrentCommitIndex())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				RequesterStepDown:  true,
			}, nil
		}

		// if my current commit index is lower than candidate current commit index
		// vote for the candidate
		if g.rafty.CurrentCommitIndex < in.GetCurrentCommitIndex() {
			g.rafty.votedFor = in.GetId()
			g.Logger.Info().Msgf("vote granted to peer %s for term %d, my current commit index %d < %d", in.GetId(), in.GetCurrentTerm(), g.rafty.CurrentCommitIndex, in.GetCurrentCommitIndex())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}

		// if my current commit index is equal to candidate current commit index and
		// if my last applied index is greater to candidate last applied index
		// reject the request vote
		if g.rafty.LastApplied > in.GetLastApplied() {
			g.Logger.Info().Msgf("reject request vote from peer %s, my last applied index %d > %d", in.GetId(), g.rafty.LastApplied, in.GetLastApplied())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				RequesterStepDown:  true,
			}, nil
		}

		// if my last applied index is lower or equal to candidate last applied index
		// vote for the candidate
		if g.rafty.LastApplied <= in.GetLastApplied() {
			g.rafty.CurrentTerm = in.CurrentTerm
			g.rafty.votedFor = in.GetId()
			g.rafty.votedForTerm = in.GetCurrentTerm()
			g.Logger.Info().Msgf("vote granted to peer %s for term %d", in.GetId(), in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}
	}

	// if I'm the current leader but the requester is not aware of it
	if g.rafty.LeaderID == g.rafty.ID {
		// As the new leader if my current term is greater than the candidate current term
		// reject the request vote
		if g.rafty.CurrentTerm > in.GetCurrentTerm() {
			g.Logger.Info().Msgf("I'm the new leader, reject request vote from peer %s, my current term %d > %d", in.GetId(), g.rafty.CurrentTerm, in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        false,
				RequesterStepDown:  true,
			}, nil
		}

		// As the new leader if my current term is lower than the candidate current term
		// set my current term to the candidate term
		// vote for the candidate
		// step down as follower
		if g.rafty.CurrentTerm < in.GetCurrentTerm() {
			g.Logger.Info().Msgf("stepping down as follower as my current term leader %d < %d", g.rafty.CurrentTerm, in.GetCurrentTerm())
			// g.rafty.CurrentTerm = in.GetCurrentTerm()
			g.rafty.votedFor = in.GetId()
			g.rafty.State = Follower
			g.rafty.votedFor = ""
			g.rafty.LeaderID = ""
			g.Logger.Info().Msgf("vote granted to peer %s for term %d", in.GetId(), in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}

		// As the new leader if my current term equal to the candidate current term and
		// if my current commit index greater than the candidate current commit index
		// reject his vote
		// tell him who is the leader
		if g.rafty.CurrentCommitIndex > in.GetCurrentCommitIndex() {
			g.Logger.Info().Msgf("I'm the new leader, reject request vote from peer %s, my current commit index %d > %d", in.GetId(), g.rafty.CurrentCommitIndex, in.GetCurrentCommitIndex())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}, nil
		}

		// As the new leader if my current commit index lower than the candidate current commit index
		// vote for the candidate
		// step down as follower
		if g.rafty.CurrentCommitIndex < in.GetCurrentCommitIndex() {
			g.rafty.votedFor = in.GetId()
			// g.rafty.State = Follower
			g.rafty.LeaderID = ""
			g.Logger.Info().Msgf("stepping down as follower, my current commit index %d < %d", g.rafty.CurrentCommitIndex, in.GetCurrentCommitIndex())
			g.Logger.Info().Msgf("vote granted to peer %s for term %d", in.GetId(), in.GetCurrentTerm())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}

		// As the new leader if my current commit index equal to the candidate current commit index and
		// if my last applied index greater than the candidate last applied index
		// reject his vote
		// tell him who is the leader
		if g.rafty.LastApplied > in.GetLastApplied() {
			g.Logger.Info().Msgf("I'm the new leader, reject request vote from peer %s, my last applied index %d > %d", in.GetId(), g.rafty.LastApplied, in.GetLastApplied())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}, nil
		}

		// As the new leader if my last applied index is lower than the candidate last applied index
		// vote for the candidate
		// step down as follower
		if g.rafty.LastApplied < in.GetLastApplied() {
			g.rafty.votedFor = in.GetId()
			// g.rafty.State = Follower
			g.rafty.LeaderID = ""
			g.Logger.Info().Msgf("vote granted to peer %s for term %d", in.GetId(), in.GetCurrentTerm())
			g.Logger.Info().Msgf("stepping down as follower as my last applied index %d < %d", g.rafty.LastApplied, in.GetLastApplied())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        true,
			}, nil
		}

		// As the new leader if my last applied index is greater than the candidate last applied index
		// reject his vote
		// I'm sticking as the leader
		// tell him who is the leader
		if g.rafty.LastApplied == in.GetLastApplied() {
			g.Logger.Info().Msgf("I'm the new leader, reject request vote from peer %s, my last applied index %d == %d", in.GetId(), g.rafty.LastApplied, in.GetLastApplied())
			return &grpcrequests.RequestVoteReply{
				CurrentTerm:        g.rafty.CurrentTerm,
				CurrentCommitIndex: g.rafty.CurrentCommitIndex,
				LastApplied:        g.rafty.LastApplied,
				PeerID:             g.rafty.ID,
				VoteGranted:        false,
				NewLeaderDetected:  true,
				RequesterStepDown:  true,
			}, nil
		}
	}

	// reject anyway
	// step down as follower
	// g.rafty.State = Follower
	g.rafty.LeaderID = ""
	g.Logger.Info().Msgf("sssstepping down as follower, my current term %d VS %d", g.rafty.CurrentTerm, in.GetCurrentTerm())
	return &grpcrequests.RequestVoteReply{
		CurrentTerm:        g.rafty.CurrentTerm,
		CurrentCommitIndex: g.rafty.CurrentCommitIndex,
		LastApplied:        g.rafty.LastApplied,
		PeerID:             g.rafty.ID,
		VoteGranted:        false,
	}, nil
}

func (g *ProtobufSVC) GetLeaderID(ctx context.Context, in *grpcrequests.GetLeader) (*grpcrequests.GetLeaderReply, error) {
	g.Logger.Info().Msgf("peer %s is looking for the leader", in.GetPeerID())
	return &grpcrequests.GetLeaderReply{Message: g.rafty.LeaderID}, nil
}

func (g *ProtobufSVC) SetLeaderID(ctx context.Context, in *grpcrequests.SetLeader) (*grpcrequests.SetLeaderReply, error) {
	g.rafty.mu.Lock()
	defer g.rafty.mu.Unlock()
	g.Logger.Info().Msgf("stepping down as follower, new leader is %s for term %d", in.GetId(), in.GetCurrentTerm())
	g.rafty.State = Follower
	g.rafty.LeaderID = in.GetId()
	lastContactDate := time.Now()
	g.rafty.LeaderLastContactDate = &lastContactDate
	g.rafty.stopElectionTimer()
	return &grpcrequests.SetLeaderReply{Message: in.GetId()}, nil
}

func (g *ProtobufSVC) SendHeartbeats(ctx context.Context, in *grpcrequests.SendHeartbeatRequest) (*grpcrequests.SendHeartbeatReply, error) {
	g.rafty.mu.Lock()
	defer g.rafty.mu.Unlock()
	// if I am the current leader and I receive heartbeat from an another leader
	if g.rafty.LeaderID != "" && g.rafty.ID == g.rafty.LeaderID && g.rafty.LeaderID != in.GetLeaderID() {
		g.Logger.Info().Msgf("Multiple leaders detected for term %d", in.GetCurrentTerm())
		return &grpcrequests.SendHeartbeatReply{
			PeerID:          g.rafty.ID,
			CurrentTerm:     g.rafty.CurrentTerm,
			MultipleLeaders: true,
		}, nil
	}
	g.Logger.Info().Msgf("heartbeat received from leader %s for term %d", in.GetLeaderID(), in.GetCurrentTerm())
	lastContactDate := time.Now()
	g.rafty.LeaderLastContactDate = &lastContactDate
	g.rafty.resetElectionTimer()
	if g.rafty.CurrentTerm < in.GetCurrentTerm() {
		g.rafty.CurrentTerm = in.GetCurrentTerm()
	}
	return &grpcrequests.SendHeartbeatReply{
		PeerID:      g.rafty.ID,
		CurrentTerm: g.rafty.CurrentTerm,
	}, nil
}

func (g *ProtobufSVC) RequestAppendEntries(ctx context.Context, in *grpcrequests.RequestAppendEntry) (*grpcrequests.RequestAppendEntryReply, error) {
	g.rafty.mu.Lock()
	defer g.rafty.mu.Unlock()
	g.Logger.Info().Msgf("append entries received from leader %s", in.GetLeaderID())
	lastContactDate := time.Now()
	g.rafty.LeaderLastContactDate = &lastContactDate
	return &grpcrequests.RequestAppendEntryReply{
		PeerID:             g.rafty.ID,
		CurrentTerm:        g.rafty.CurrentTerm,
		CurrentCommitIndex: g.rafty.CurrentCommitIndex,
		LastApplied:        g.rafty.LastApplied,
		Ack:                true,
	}, nil
}
