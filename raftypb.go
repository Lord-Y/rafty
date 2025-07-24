package rafty

import (
	"context"
	"fmt"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// AskNodeID allow the current node to exchange its node id
// with other nodes. It will also provide the leader address and id
// if found.
// If the requester is not part of the current cluster, membership will
// be set to true
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
		PeerId:           r.rafty.id,
		ReadReplica:      r.rafty.options.ReadReplica,
		LeaderId:         lid,
		LeaderAddress:    lad,
		AskForMembership: mustAskForMembership,
	}, nil
}

// GetLeader is used only between nodes to find leader address and id.
// If the requester is not part of the current cluster, membership will
// be set to true
func (r *rpcManager) GetLeader(ctx context.Context, in *raftypb.GetLeaderRequest) (*raftypb.GetLeaderResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	var mustAskForMembership bool
	if !r.rafty.isPartOfTheCluster(peer{ID: in.PeerId, Address: in.PeerAddress}) {
		mustAskForMembership = true
	}

	response := &raftypb.GetLeaderResponse{
		PeerId:           r.rafty.id,
		AskForMembership: mustAskForMembership,
	}

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("peerAddress", in.PeerAddress).
		Str("peerId", in.PeerId).
		Str("mustAskForMembership", fmt.Sprintf("%t", mustAskForMembership)).
		Msgf("Peer is looking for the leader")

	if r.rafty.getState() == Leader {
		response.LeaderId = r.rafty.id
		response.LeaderAddress = r.rafty.Address.String()
		return response, nil
	}

	leader := r.rafty.getLeader()
	response.LeaderId = leader.id
	response.LeaderAddress = leader.address
	return response, nil
}

// SendPreVoteRequest allow the current node received
// and treat pre vote requests from other nodes
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

// SendVoteRequest allow the current node received
// and treat vote requests from other nodes
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

// SendAppendEntriesRequest allow the current node received
// and treat append entries requests from the leader
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

// ClientGetLeader is used only by clients to find leader address and id
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
		LeaderId:      lid,
		LeaderAddress: lad,
	}, nil
}

// ForwardCommandToLeader allow the current node received
// and forward commands to the leader.
// If the current node is not the leader, it will provide leader address and id
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

// SendTimeoutNowRequest allow the current node received
// and treat leadership transfer from the leader when it
// needs to step down
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

// SendMembershipChangeRequest allow the current leader node received
// and treat membership requests from other nodes or devops to manage the cluster
func (r *rpcManager) SendMembershipChangeRequest(ctx context.Context, in *raftypb.MembershipChangeRequest) (*raftypb.MembershipChangeResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}
	if r.rafty.options.BootstrapCluster && !r.rafty.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
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

	case <-time.After(membershipTimeoutSeconds * time.Second):
		return nil, errTimeoutSendingRequest
	}
}

// SendBootstrapClusterRequest allow the current node to bootstrap the cluster
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
