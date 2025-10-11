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
	if !r.rafty.isPartOfTheCluster(Peer{ID: in.Id, Address: in.Address}) {
		mustAskForMembership = true
	}

	return &raftypb.AskNodeIDResponse{
		PeerId:           r.rafty.id,
		IsVoter:          r.rafty.options.IsVoter,
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
	if !r.rafty.isPartOfTheCluster(Peer{ID: in.PeerId, Address: in.PeerAddress}) {
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
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.PreVoteResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, ErrTimeout
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
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.VoteResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, ErrTimeout
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
	case r.rafty.rpcAppendEntriesReplicationRequestChan <- RPCRequest{
		RPCType:      AppendEntriesReplicationRequest,
		Request:      in,
		ResponseChan: responseChan,
	}:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.AppendEntryResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, ErrTimeout
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

	timeout := time.Second
	var request RPCRequest
	responseChan := make(chan RPCResponse, 1)
	switch LogKind(in.LogType) {
	case LogCommandReadLeader:
		if r.rafty.IsLeader() {
			response, err := r.rafty.fsm.ApplyCommand(&LogEntry{LogType: in.LogType, Command: in.Command})
			return &raftypb.ForwardCommandToLeaderResponse{
				LeaderId:      r.rafty.id,
				LeaderAddress: r.rafty.Address.String(),
				Data:          response,
				Error:         fmt.Sprintf("%v", err),
			}, err
		}

		return &raftypb.ForwardCommandToLeaderResponse{}, ErrNotLeader

	case LogConfiguration:
		if r.rafty.getState() != Leader {
			return &raftypb.ForwardCommandToLeaderResponse{
				LeaderId:      r.rafty.id,
				LeaderAddress: r.rafty.Address.String(),
			}, ErrNotLeader
		}

		if in.MembershipTimeout != 0 {
			timeout = time.Duration(in.MembershipTimeout) * time.Second
		}
		decodePeers, err := DecodePeers(in.Command)
		if err != nil {
			return nil, err
		}
		decodePeers[0].address = getNetAddress(decodePeers[0].Address)

		request = RPCRequest{
			RPCType: ForwardCommandToLeader,
			Request: replicateLogConfig{
				logType:    LogConfiguration,
				source:     "forward",
				client:     true,
				clientChan: responseChan,
				membershipChange: struct {
					action MembershipChange
					member Peer
				}{
					member: decodePeers[0],
					action: MembershipChange(in.MembershipAction),
				},
			},
			ResponseChan: responseChan,
		}

	default:
		request = RPCRequest{
			RPCType: ForwardCommandToLeader,
			Request: replicateLogConfig{
				logType:    LogReplication,
				command:    in.Command,
				source:     "forward",
				client:     true,
				clientChan: responseChan,
			},
			ResponseChan: responseChan,
		}
	}

	select {
	case r.rafty.rpcAppendEntriesRequestChan <- request:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.ForwardCommandToLeaderResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(timeout):
		return nil, ErrTimeout
	}
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
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.BootstrapClusterResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(time.Second):
		return nil, ErrTimeout
	}
}

// SendInstallSnapshotRequest allow the current node to bootstrap the cluster
func (r *rpcManager) SendInstallSnapshotRequest(ctx context.Context, in *raftypb.InstallSnapshotRequest) (*raftypb.InstallSnapshotResponse, error) {
	if r.rafty.getState() == Down || !r.rafty.isRunning.Load() || r.rafty.quitCtx.Err() != nil {
		return nil, ErrShutdown
	}

	responseChan := make(chan RPCResponse, 1)
	select {
	case r.rafty.rpcInstallSnapshotRequestChan <- RPCRequest{
		RPCType:      InstallSnapshotRequest,
		Request:      in,
		ResponseChan: responseChan,
	}:

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.InstallSnapshotResponse), response.Error

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.rafty.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(5 * time.Second):
		return nil, ErrTimeout
	}
}
