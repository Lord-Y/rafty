package rafty

import (
	"context"
	"slices"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
)

// preVoteRequest connect to peers in order to check who is the leader
// If no leader, fetch their currentTerm
// and decided if they are suitable for election campain
func (r *Rafty) preVoteRequest() {
	myAddress, myId := r.getMyAddress()
	currentTerm := r.getCurrentTerm()
	state := r.getState()
	r.mu.Lock()
	r.PreCandidatePeers = nil
	peers := r.Peers
	r.mu.Unlock()

	for _, peer := range peers {
		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.getState() != Down && r.leaderLost.Load() {
			if !r.healthyPeer(peer) {
				return
			}
			go func() {
				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s with term %d for pre vote request", myAddress, myId, state.String(), peer.address.String(), currentTerm)

				response, err := peer.rclient.SendPreVoteRequest(
					context.Background(),
					&raftypb.PreVoteRequest{
						Id:          myId,
						State:       state.String(),
						CurrentTerm: currentTerm,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.preVoteResponseErrorChan <- voteResponseErrorWrapper{
						peer: peer,
						err:  err,
					}
					return
				}
				r.preVoteResponseChan <- preVoteResponseWrapper{
					peer:     peer,
					response: response,
				}
			}()
		}
	}
}

// startElection permit to send vote request
// to other nodes in order to elect a leader
func (r *Rafty) startElection() {
	r.resetElectionTimer(false, true)
	myAddress, myId := r.getMyAddress()
	currentTerm := r.incrementCurrentTerm()
	state := r.getState()
	lastLogIndex := r.getLastLogIndex()
	r.mu.Lock()
	preCandidatePeers := r.PreCandidatePeers
	r.quoroms = nil
	r.mu.Unlock()
	if err := r.persistMetadata(); err != nil {
		r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
	}
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = r.getX(r.log[lastLogIndex].Term)
	}

	r.Logger.Trace().Msgf("Me %s / %s starting election campain with term %d and peers %+v", myAddress, myId, currentTerm, preCandidatePeers)
	for _, peer := range preCandidatePeers {
		if peer.id == "" {
			r.Logger.Info().Msgf("Peer %s has no id so we cannot start the election", peer.address.String())
			r.switchState(Follower, true, currentTerm)
			r.resetElectionTimer(false, true)
			return
		}

		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.getState() == Candidate && r.leaderLost.Load() {
			if !r.healthyPeer(peer) {
				return
			}

			go func() {
				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s / %s with term %d for election campain", myAddress, myId, state.String(), peer.address.String(), peer.id, currentTerm)

				response, err := peer.rclient.SendVoteRequest(
					context.Background(),
					&raftypb.VoteRequest{
						CandidateId:      myId,
						CandidateAddress: myAddress,
						State:            state.String(),
						CurrentTerm:      currentTerm,
						LastLogIndex:     lastLogIndex,
						LastLogTerm:      lastLogTerm,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)

				if err != nil {
					r.voteResponseErrorChan <- voteResponseErrorWrapper{
						peer: peer,
						err:  err,
					}
					return
				}

				r.voteResponseChan <- voteResponseWrapper{
					peer:             peer,
					response:         response,
					savedCurrentTerm: currentTerm,
				}
			}()
		}
	}
}
