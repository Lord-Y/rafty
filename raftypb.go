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

	var mustAskForMembership bool
	if !r.rafty.isPartOfTheCluster(peer{ID: in.Id, Address: in.Address}) {
		mustAskForMembership = true
	}

	return &raftypb.AskNodeIDResponse{
		PeerID:           r.rafty.id,
		ReadReplica:      r.rafty.options.ReadReplica,
		LeaderID:         lid,
		LeaderAddress:    lad,
		AskForMembership: mustAskForMembership,
	}, nil
}

func (r *rpcManager) GetLeader(ctx context.Context, in *raftypb.GetLeaderRequest) (*raftypb.GetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	var mustAskForMembership bool
	if !r.rafty.isPartOfTheCluster(peer{ID: in.PeerID, Address: in.PeerAddress}) {
		mustAskForMembership = true
	}

	response := &raftypb.GetLeaderResponse{
		PeerID:           r.rafty.id,
		AskForMembership: mustAskForMembership,
	}

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("peerAddress", in.PeerAddress).
		Str("peerId", in.PeerID).
		Str("mustAskForMembership", fmt.Sprintf("%t", mustAskForMembership)).
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

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcPreVoteRequestChan <- RPCRequest{
		RPCType:      PreVoteRequest,
		Request:      in,
		ResponseChan: responseChan,
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
		return response.Response.(*raftypb.PreVoteResponse), response.Error

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

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcVoteRequestChan <- RPCRequest{
		RPCType:      VoteRequest,
		Request:      in,
		ResponseChan: responseChan,
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
		return response.Response.(*raftypb.VoteResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, errTimeoutSendingRequest
	}
}

func (r *rpcManager) SendAppendEntriesRequest(ctx context.Context, in *raftypb.AppendEntryRequest) (*raftypb.AppendEntryResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcAppendEntriesRequestChan <- RPCRequest{
		RPCType:      AppendEntryRequest,
		Request:      in,
		ResponseChan: responseChan,
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
		return response.Response.(*raftypb.AppendEntryResponse), response.Error

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
	if r.rafty.options.BootstrapCluster && !r.rafty.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
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
	if r.rafty.options.BootstrapCluster && !r.rafty.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
	}

	cmd, err := decodeCommand(in.Command)
	if err != nil {
		return nil, err
	}
	if cmd.Kind == CommandSet {
		responseChan := make(chan RPCResponse, 1)
		select {
		case r.rafty.rpcForwardCommandToLeaderRequestChan <- RPCRequest{
			RPCType:      ForwardCommandToLeader,
			Request:      in,
			ResponseChan: responseChan,
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
			return response.Response.(*raftypb.ForwardCommandToLeaderResponse), response.Error

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

func (r *rpcManager) SendTimeoutNowRequest(_ context.Context, in *raftypb.TimeoutNowRequest) (*raftypb.TimeoutNowResponse, error) {
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

func (r *rpcManager) SendMembershipChangeRequest(ctx context.Context, in *raftypb.MembershipChangeRequest) (*raftypb.MembershipChangeResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcMembershipChangeRequestChan <- RPCRequest{
		RPCType:      MembershipChangeRequest,
		Request:      in,
		ResponseChan: responseChan,
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
		return response.Response.(*raftypb.MembershipChangeResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second * membershipTimeoutSeconds):
		return nil, errTimeoutSendingRequest
	}
}

func (r *rpcManager) SendBootstrapClusterRequest(ctx context.Context, in *raftypb.BootstrapClusterRequest) (*raftypb.BootstrapClusterResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcBootstrapClusterRequestChan <- RPCRequest{
		RPCType:      BootstrapClusterRequest,
		Request:      in,
		ResponseChan: responseChan,
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
		return response.Response.(*raftypb.BootstrapClusterResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, errTimeoutSendingRequest
	}
}
