package rafty

import (
	"context"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *rpcManager) AskNodeID(_ context.Context, in *raftypb.AskNodeIDRequest) (*raftypb.AskNodeIDResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	leader := r.rafty.getLeader()
	var lid, lad string
	switch {
	case r.rafty.getState() == Leader:
		lid, lad = r.rafty.id, r.rafty.Address.String()
	default:
		lid, lad = leader.id, leader.address
	}

	return &raftypb.AskNodeIDResponse{
		PeerID:        r.rafty.id,
		ReadOnlyNode:  r.rafty.options.ReadOnlyNode,
		LeaderID:      lid,
		LeaderAddress: lad,
	}, nil
}

func (r *rpcManager) GetLeader(_ context.Context, in *raftypb.GetLeaderRequest) (*raftypb.GetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	response := &raftypb.GetLeaderResponse{
		PeerID: r.rafty.id,
	}

	r.rafty.Logger.Info().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("peerAddress", in.PeerAddress).
		Str("peerId", in.PeerID).
		Msgf("Peer is looking for the leader")

	if r.rafty.getState() == Leader {
		// go r.rafty.connectionManager.getClient(in.PeerAddress, in.PeerID)
		response.LeaderID = r.rafty.id
		response.LeaderAddress = r.rafty.Address.String()
		return response, nil
	}

	leader := r.rafty.getLeader()
	response.LeaderID = leader.id
	response.LeaderAddress = leader.address
	return response, nil
}

func (r *rpcManager) SendPreVoteRequest(_ context.Context, in *raftypb.PreVoteRequest) (*raftypb.PreVoteResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	responseChan := make(chan *raftypb.PreVoteResponse, 1)
	r.rafty.rpcPreVoteRequestChan <- preVoteResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}
	response := <-responseChan
	return response, nil
}

func (r *rpcManager) SendVoteRequest(_ context.Context, in *raftypb.VoteRequest) (*raftypb.VoteResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	responseChan := make(chan *raftypb.VoteResponse, 1)
	r.rafty.rpcVoteRequestChan <- voteResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}
	response := <-responseChan
	return response, nil
}

func (r *rpcManager) SendAppendEntriesRequest(_ context.Context, in *raftypb.AppendEntryRequest) (*raftypb.AppendEntryResponse, error) {
	responseChan := make(chan *raftypb.AppendEntryResponse, 1)
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	r.rafty.rpcAppendEntriesRequestChan <- appendEntriesResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}
	response := <-responseChan
	return response, nil
}

func (r *rpcManager) ClientGetLeader(_ context.Context, in *raftypb.ClientGetLeaderRequest) (*raftypb.ClientGetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	state := r.rafty.getState()
	leader := r.rafty.getLeader()
	var lid, lad string
	if state == Leader {
		lid, lad = r.rafty.id, r.rafty.Address.String()
	} else {
		lid, lad = leader.id, leader.address
	}

	return &raftypb.ClientGetLeaderResponse{
		LeaderID:      lid,
		LeaderAddress: lad,
	}, nil
}

func (r *rpcManager) ForwardCommandToLeader(_ context.Context, in *raftypb.ForwardCommandToLeaderRequest) (*raftypb.ForwardCommandToLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() {
		return nil, ErrShutdown
	}

	cmd, err := decodeCommand(in.Command)
	if err != nil {
		return nil, err
	}
	if cmd.Kind == CommandSet {
		responseChan := make(chan *raftypb.ForwardCommandToLeaderResponse, 1)
		r.rafty.rpcForwardCommandToLeaderRequestChan <- forwardCommandToLeaderRequestWrapper{
			request:      in,
			responseChan: responseChan,
		}
		response := <-responseChan
		return response, nil
	}
	return &raftypb.ForwardCommandToLeaderResponse{}, nil
}
