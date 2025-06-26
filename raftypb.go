package rafty

import (
	"context"
	"fmt"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *rpcManager) AskNodeID(ctx context.Context, in *raftypb.AskNodeIDRequest) (*raftypb.AskNodeIDResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
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

func (r *rpcManager) GetLeader(ctx context.Context, in *raftypb.GetLeaderRequest) (*raftypb.GetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
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
		response.LeaderID = r.rafty.id
		response.LeaderAddress = r.rafty.Address.String()
		return response, nil
	}

	leader := r.rafty.getLeader()
	response.LeaderID = leader.id
	response.LeaderAddress = leader.address
	return response, nil
}

func (r *rpcManager) SendPreVoteRequest(ctx context.Context, in *raftypb.PreVoteRequest) (*raftypb.PreVoteResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan *raftypb.PreVoteResponse, 1)
	select {
	case r.rafty.rpcPreVoteRequestChan <- preVoteResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, errTimeoutSendingRequest
	}

	select {
	case response := <-responseChan:
		return response, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, errTimeoutSendingRequest
	}
}

func (r *rpcManager) SendVoteRequest(ctx context.Context, in *raftypb.VoteRequest) (*raftypb.VoteResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan *raftypb.VoteResponse, 1)
	select {
	case r.rafty.rpcVoteRequestChan <- voteResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, errTimeoutSendingRequest
	}

	select {
	case response := <-responseChan:
		return response, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, errTimeoutSendingRequest
	}
}

func (r *rpcManager) SendAppendEntriesRequest(ctx context.Context, in *raftypb.AppendEntryRequest) (*raftypb.AppendEntryResponse, error) {
	responseChan := make(chan *raftypb.AppendEntryResponse, 1)
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	select {
	case r.rafty.rpcAppendEntriesRequestChan <- appendEntriesResquestWrapper{
		request:      in,
		responseChan: responseChan,
	}:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, errTimeoutSendingRequest
	}

	select {
	case response := <-responseChan:
		return response, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, errTimeoutSendingRequest
	}
}

func (r *rpcManager) ClientGetLeader(ctx context.Context, in *raftypb.ClientGetLeaderRequest) (*raftypb.ClientGetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
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

func (r *rpcManager) ForwardCommandToLeader(ctx context.Context, in *raftypb.ForwardCommandToLeaderRequest) (*raftypb.ForwardCommandToLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	cmd, err := decodeCommand(in.Command)
	if err != nil {
		return nil, err
	}
	if cmd.Kind == CommandSet {
		responseChan := make(chan *raftypb.ForwardCommandToLeaderResponse, 1)

		select {
		case r.rafty.rpcForwardCommandToLeaderRequestChan <- forwardCommandToLeaderRequestWrapper{
			request:      in,
			responseChan: responseChan,
		}:

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-r.rafty.quitCtx.Done():
			return nil, ErrShutdown

		case <-time.After(500 * time.Millisecond):
			return nil, errTimeoutSendingRequest
		}

		select {
		case response := <-responseChan:
			return response, nil

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-r.rafty.quitCtx.Done():
			return nil, ErrShutdown

		case <-time.After(time.Second):
			return nil, errTimeoutSendingRequest
		}
	}
	return nil, nil
}

func (r *rpcManager) SendTimeoutNowRequest(ctx context.Context, in *raftypb.TimeoutNowRequest) (*raftypb.TimeoutNowResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil || ctx.Done() != nil {
		return nil, ErrShutdown
	}

	r.rafty.candidateForLeadershipTransfer.Store(true)
	r.rafty.switchState(Candidate, stepUp, false, r.rafty.currentTerm.Load()+1)
	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("candidateForLeadershipTransfer", fmt.Sprintf("%t", r.rafty.candidateForLeadershipTransfer.Load())).
		Msg("LeadershipTransfer received")
	return &raftypb.TimeoutNowResponse{Success: true}, nil
}
