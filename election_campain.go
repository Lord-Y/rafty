package rafty

import (
	"context"
	"fmt"
	"slices"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// preVoteRequest connect to peers in order to check who is the leader
// If no leader, fetch their currentTerm
// and decided if they are suitable for election campain
func (r *Rafty) preVoteRequest() {
	currentTerm := r.getCurrentTerm()
	state := r.getState()
	r.mu.Lock()
	r.configuration.preCandidatePeers = nil
	peers := r.configuration.ServerMembers
	r.mu.Unlock()

	askPeer := func(peer peer) {
		if peer.client != nil && !peer.ReadOnlyNode && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.getState() != Down && r.leaderLost.Load() {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", peer.address.String()).
				Str("peerId", peer.ID).
				Msgf("Send pre vote request")

			var (
				response *raftypb.PreVoteResponse
				err      error
			)
			if response, err = peer.rclient.SendPreVoteRequest(
				context.Background(),
				&raftypb.PreVoteRequest{
					Id:          r.id,
					State:       state.String(),
					CurrentTerm: currentTerm,
				},
				grpc.WaitForReady(true),
				grpc.UseCompressor(gzip.Name),
			); err != nil {
				if r.getState() != Down {
					r.Logger.Error().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Msgf("Fail to get pre vote request")
				}
				return
			}
			r.preVoteResponseChan <- preVoteResponseWrapper{
				peer:     peer,
				response: response,
			}
		}
	}

	for _, peer := range peers {
		go askPeer(peer)
	}
}

// startElection permit to send vote request
// to other nodes in order to elect a leader
func (r *Rafty) startElection() {
	r.resetElectionTimer()
	myAddress, myId := r.getMyAddress()
	currentTerm := r.incrementCurrentTerm()
	state := r.getState()
	lastLogIndex := r.getLastLogIndex()
	r.mu.Lock()
	preCandidatePeers := r.configuration.preCandidatePeers
	r.quoroms = nil
	r.mu.Unlock()
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = r.getX(r.log[lastLogIndex].Term)
	}

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("term", fmt.Sprintf("%d", currentTerm)).
		Msgf("Start election campain")

	askPeer := func(peer peer) {
		if peer.client != nil && !peer.ReadOnlyNode && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.getState() != Down && r.leaderLost.Load() {
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", currentTerm)).
				Str("peerAddress", peer.address.String()).
				Str("peerId", peer.ID).
				Msgf("Send vote request")

			var (
				response *raftypb.VoteResponse
				err      error
			)
			if response, err = peer.rclient.SendVoteRequest(
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
			); err != nil {
				if r.getState() != Down {
					r.Logger.Error().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Str("statusCode", status.Code(err).String()).
						Msgf("Fail to send vote request to peer")
				}
				return
			}
			r.voteResponseChan <- voteResponseWrapper{
				peer:             peer,
				response:         response,
				savedCurrentTerm: currentTerm,
			}
		}
	}

	for _, peer := range preCandidatePeers {
		go askPeer(peer)
	}
}
