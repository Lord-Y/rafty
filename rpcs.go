package rafty

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
)

// RPCType is used to build rpc requests
type RPCType uint8

const (
	// AskNodeID will be used to ask node id
	AskNodeID RPCType = iota

	// GetLeader will be used to ask who is the leader
	GetLeader

	// PreVoteRequest is used during pre vote request
	PreVoteRequest

	// VoteRequest is used during Vote request
	VoteRequest

	// AppendEntryRequest is used during append entry request
	AppendEntryRequest

	// ForwardCommandToLeader is used to forward command to leader
	ForwardCommandToLeader

	// TimeoutNow is used during leadership transfer
	TimeoutNowRequest

	// MembershipChangeRequest is used during membership changes
	// by a new node that wants to be part of the cluster
	MembershipChangeRequest

	// BootstrapClusterRequest is used to bootstrap the cluster
	BootstrapClusterRequest
)

// RPCRequest is used by chans in order to manage rpc requests
type RPCRequest struct {
	RPCType      RPCType
	Request      any
	Timeout      time.Duration
	ResponseChan chan<- RPCResponse
}

// RPCResponse  is used by RPCRequest in order to reply to rpc requests
type RPCResponse struct {
	TargetPeer peer
	Response   any
	Error      error
}

// RPCAskNodeIDRequest hold the requirements to ask node id
type RPCAskNodeIDRequest struct {
	Id, Address string
}

// RPCAskNodeIDResponse hold the response from RPCAskNodeIDRequest
type RPCAskNodeIDResponse struct {
	LeaderID, LeaderAddress, PeerID string
	ReadReplica, AskForMembership   bool
}

// RPCGetLeaderRequest hold the requirements to get the leader
type RPCGetLeaderRequest struct {
	PeerID, PeerAddress string
	TotalPeers          int
}

// RPCGetLeaderResponse hold the response from RPCGetLeaderRequest
type RPCGetLeaderResponse struct {
	LeaderID, LeaderAddress, PeerID string
	TotalPeers                      int
	AskForMembership                bool
}

// RPCPreVoteRequest hold the requirements to send pre vote requests
type RPCPreVoteRequest struct {
	Id          string
	CurrentTerm uint64
}

// RPCPreVoteResponse hold the response from RPCPreVoteRequest
type RPCPreVoteResponse struct {
	PeerID                     string
	RequesterTerm, CurrentTerm uint64
	Granted                    bool
}

// RPCVoteRequest hold the requirements to send vote requests
type RPCVoteRequest struct {
	CandidateId, CandidateAddress          string
	CurrentTerm, LastLogIndex, LastLogTerm uint64
	CandidateForLeadershipTransfer         bool
}

// RPCVoteResponse hold the response from RPCVoteRequest
type RPCVoteResponse struct {
	PeerID                     string
	RequesterTerm, CurrentTerm uint64
	Granted                    bool
}

// RPCTimeoutNowRequest hold the requirements to send timeout now requests
// for leadership transfer
type RPCTimeoutNowRequest struct{}

// RPCTimeoutNowResponse hold the response from RPCTimeoutNowRequest
type RPCTimeoutNowResponse struct {
	Success bool
}

// RPCMembershipChangeRequest hold the requirements to send membership requests
type RPCMembershipChangeRequest struct {
	Address, Id               string
	ReadReplica               bool
	Action                    uint32
	LastLogIndex, LastLogTerm uint64
}

// RPCMembershipChangeResponse hold the response from RPCMembershipChangeRequest
type RPCMembershipChangeResponse struct {
	ActionPerformed, Response uint32
	LeaderID, LeaderAddress   string
	Peers                     []peer
	Success                   bool
}

// sendRPC is use to send rpc request
func (r *Rafty) sendRPC(request RPCRequest, client raftypb.RaftyClient, peer peer) {
	options := []grpc.CallOption{}

	ctx := context.Background()
	var cancel context.CancelFunc
	if request.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), request.Timeout)
		defer cancel()
	}

	switch request.RPCType {
	case AskNodeID:
		resp, err := client.AskNodeID(
			ctx,
			makeRPCAskNodeIDRequest(request.Request.(RPCAskNodeIDRequest)),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCAskNodeIDResponse(resp), Error: err, TargetPeer: peer}

	case GetLeader:
		req := request.Request.(RPCGetLeaderRequest)
		resp, err := client.GetLeader(
			ctx,
			makeRPCGetLeaderRequest(req),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCGetLeaderResponse(resp, req.TotalPeers), Error: err, TargetPeer: peer}

	case PreVoteRequest:
		req := request.Request.(RPCPreVoteRequest)
		resp, err := client.SendPreVoteRequest(
			ctx,
			makeRPCPreVoteRequest(req),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCPreVoteResponse(resp, req.CurrentTerm), Error: err, TargetPeer: peer}

	case VoteRequest:
		req := request.Request.(RPCVoteRequest)
		resp, err := client.SendVoteRequest(
			ctx,
			makeRPCVoteRequest(req),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCVoteResponse(resp, req.CurrentTerm), Error: err, TargetPeer: peer}

	case TimeoutNowRequest:
		resp, err := client.SendTimeoutNowRequest(
			ctx,
			&raftypb.TimeoutNowRequest{},
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCTimeoutNowResponse(resp), Error: err, TargetPeer: peer}

	case MembershipChangeRequest:
		req := request.Request.(RPCMembershipChangeRequest)
		resp, err := client.SendMembershipChangeRequest(
			ctx,
			makeRPCMembershipChangeRequest(req),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCMembershipChangeResponse(resp, req.Action), Error: err, TargetPeer: peer}

	default:
		request.ResponseChan <- RPCResponse{Error: errUnkownRPCType, TargetPeer: peer}
	}
}

// makeRPCAskNodeIDRequest build ask node id request
func makeRPCAskNodeIDRequest(data RPCAskNodeIDRequest) *raftypb.AskNodeIDRequest {
	return &raftypb.AskNodeIDRequest{
		Id:      data.Id,
		Address: data.Address,
	}
}

// makeRPCAskNodeIDResponse ask node id response
func makeRPCAskNodeIDResponse(data *raftypb.AskNodeIDResponse) RPCAskNodeIDResponse {
	// at first, data will be nil because the target node
	// is not yet reachable
	if data == nil {
		return RPCAskNodeIDResponse{}
	}
	return RPCAskNodeIDResponse{
		LeaderID:         data.LeaderId,
		LeaderAddress:    data.LeaderAddress,
		PeerID:           data.PeerId,
		ReadReplica:      data.ReadReplica,
		AskForMembership: data.AskForMembership,
	}
}

// makeRPCGetLeaderRequest build get leader request
func makeRPCGetLeaderRequest(data RPCGetLeaderRequest) *raftypb.GetLeaderRequest {
	return &raftypb.GetLeaderRequest{
		PeerId:      data.PeerID,
		PeerAddress: data.PeerAddress,
	}
}

// makeRPCGetLeaderResponse build get leader response
func makeRPCGetLeaderResponse(data *raftypb.GetLeaderResponse, totalPeers int) RPCGetLeaderResponse {
	if data == nil {
		return RPCGetLeaderResponse{}
	}
	return RPCGetLeaderResponse{
		LeaderID:         data.LeaderId,
		LeaderAddress:    data.LeaderAddress,
		PeerID:           data.PeerId,
		TotalPeers:       totalPeers,
		AskForMembership: data.AskForMembership,
	}
}

// makeRPCPreVoteRequest build pre vote request
func makeRPCPreVoteRequest(data RPCPreVoteRequest) *raftypb.PreVoteRequest {
	return &raftypb.PreVoteRequest{
		Id:          data.Id,
		CurrentTerm: data.CurrentTerm,
	}
}

// makeRPCPreVoteResponse build pre vote response
func makeRPCPreVoteResponse(data *raftypb.PreVoteResponse, term uint64) RPCPreVoteResponse {
	if data == nil {
		return RPCPreVoteResponse{RequesterTerm: term}
	}
	return RPCPreVoteResponse{
		RequesterTerm: term,
		CurrentTerm:   data.CurrentTerm,
		Granted:       data.Granted,
	}
}

// makeRPCVoteRequest build vote request
func makeRPCVoteRequest(data RPCVoteRequest) *raftypb.VoteRequest {
	return &raftypb.VoteRequest{
		CandidateId:      data.CandidateId,
		CandidateAddress: data.CandidateAddress,
		CurrentTerm:      data.CurrentTerm,
		LastLogIndex:     data.LastLogIndex,
		LastLogTerm:      data.LastLogTerm,
	}
}

// makeRPCVoteResponse build vote response
func makeRPCVoteResponse(data *raftypb.VoteResponse, term uint64) RPCVoteResponse {
	if data == nil {
		return RPCVoteResponse{RequesterTerm: term}
	}
	return RPCVoteResponse{
		PeerID:        data.PeerId,
		RequesterTerm: term,
		CurrentTerm:   data.CurrentTerm,
		Granted:       data.Granted,
	}
}

// makeRPCTimeoutNowResponse build timeout now response
func makeRPCTimeoutNowResponse(data *raftypb.TimeoutNowResponse) RPCTimeoutNowResponse {
	if data == nil {
		return RPCTimeoutNowResponse{}
	}
	return RPCTimeoutNowResponse{
		Success: data.Success,
	}
}

// makeRPCMembershipChangeRequest build membership change request
func makeRPCMembershipChangeRequest(data RPCMembershipChangeRequest) *raftypb.MembershipChangeRequest {
	return &raftypb.MembershipChangeRequest{
		Id:          data.Id,
		Address:     data.Address,
		ReadReplica: data.ReadReplica,
		Action:      data.Action,
	}
}

// makeRPCMembershipChangeResponse build membership change response
func makeRPCMembershipChangeResponse(data *raftypb.MembershipChangeResponse, action uint32) RPCMembershipChangeResponse {
	if data == nil {
		return RPCMembershipChangeResponse{ActionPerformed: action, Success: false}
	}
	return RPCMembershipChangeResponse{
		ActionPerformed: action,
		LeaderID:        data.LeaderId,
		LeaderAddress:   data.LeaderAddress,
		Success:         data.Success,
	}
}

// sendAskNodeIDRequest will ask targeted node its id
func (r *Rafty) sendAskNodeIDRequest() {
	peers, _ := r.getPeers()

	request := RPCRequest{
		RPCType: AskNodeID,
		Request: RPCAskNodeIDRequest{
			Id:      r.id,
			Address: r.Address.String(),
		},
		Timeout:      time.Second,
		ResponseChan: r.rpcAskNodeIDChan,
	}

	for _, peer := range peers {
		go func() {
			client := r.connectionManager.getClient(peer.address.String(), peer.ID)
			if client != nil && r.getState() != Down {
				r.sendRPC(request, client, peer)
			}
		}()
	}
}

// askNodeIDResult will handle askNodeID result
func (r *Rafty) askNodeIDResult(resp RPCResponse) {
	response := resp.Response.(RPCAskNodeIDResponse)
	err := resp.Error
	targetPeer := resp.TargetPeer

	if r.getState() != Down && err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Msgf("Fail to fetch peer id")
		return
	}

	r.mu.Lock()
	if index := slices.IndexFunc(r.configuration.ServerMembers, func(peer peer) bool {
		return peer.address.String() == targetPeer.address.String() && peer.ID == ""
	}); index != -1 {
		r.configuration.ServerMembers[index].ID = response.PeerID
		r.configuration.ServerMembers[index].ReadReplica = response.ReadReplica
	}
	peers := r.configuration.ServerMembers
	r.mu.Unlock()

	if response.AskForMembership {
		r.askForMembership.Store(true)
	}

	// checking if all servers have an id
	// if not, we are not ready
	for index := range peers {
		if peers[index].ID == "" {
			return
		}
	}

	if response.LeaderAddress != "" && response.LeaderID != "" {
		r.setLeader(leaderMap{address: response.LeaderAddress, id: response.LeaderID})
		r.leaderLastContactDate.Store(time.Now())
	}

	if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !response.ReadReplica {
		r.clusterSizeCounter.Add(1)
	}
}

// sendGetLeaderRequest allow the current node
// ask to other nodes who is the actual leader
// and prevent starting election campaign
func (r *Rafty) sendGetLeaderRequest() {
	peers, totalPeers := r.getPeers()
	r.leaderCount.Add(0)
	r.leaderFound.Store(false)

	request := RPCRequest{
		RPCType: GetLeader,
		Request: RPCGetLeaderRequest{
			PeerID:      r.id,
			PeerAddress: r.Address.String(),
			TotalPeers:  totalPeers,
		},
		Timeout:      time.Second,
		ResponseChan: r.rpcClientGetLeaderChan,
	}

	for _, peer := range peers {
		go func() {
			client := r.connectionManager.getClient(peer.address.String(), peer.ID)
			if client != nil && r.getState() != Down {
				r.sendRPC(request, client, peer)
			}
		}()
	}
}

// getLeaderResult allow the current node
// ask to other nodes who is the actual leader
// and prevent starting election campaign
func (r *Rafty) getLeaderResult(resp RPCResponse) {
	response := resp.Response.(RPCGetLeaderResponse)
	err := resp.Error
	targetPeer := resp.TargetPeer

	if r.getState() != Down && err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Msgf("Fail to ask this peer who is the leader")
		return
	}

	if r.leaderFound.Load() {
		return
	}

	r.leaderCount.Add(1)
	if response.LeaderID != "" {
		r.leaderFound.Store(true)
		r.setLeader(leaderMap{
			address: response.LeaderAddress,
			id:      response.LeaderID,
		})

		r.Logger.Debug().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("leaderAddress", response.LeaderAddress).
			Str("leaderId", response.LeaderID).
			Msgf("Leader found")
		r.leaderLastContactDate.Store(time.Now())
	}

	if !r.leaderFound.Load() && int(r.leaderCount.Load()) == response.TotalPeers {
		r.Logger.Debug().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("There is no leader")
	}
}

// sendMembershipChangeRequest is used by the current node
// to ask leader to be part of the cluster
func (r *Rafty) sendMembershipChangeRequest(action MembershipChange) {
	request := RPCRequest{
		RPCType: MembershipChangeRequest,
		Request: RPCMembershipChangeRequest{
			Id:           r.id,
			Address:      r.Address.String(),
			Action:       uint32(action),
			LastLogIndex: r.lastLogIndex.Load(),
			LastLogTerm:  r.lastLogTerm.Load(),
			ReadReplica:  r.options.ReadReplica,
		},
		Timeout:      time.Second,
		ResponseChan: r.rpcMembershipChangeChan,
	}

	leader := r.getLeader()
	if leader.address != "" && leader.id != "" {
		peer := peer{
			Address: leader.address,
			address: getNetAddress(leader.address),
			ID:      leader.id,
		}
		client := r.connectionManager.getClient(peer.Address, leader.id)
		if client != nil {
			// membership is a kind of a long running progress
			// so we need to send the request in background
			// to be able to receive calls from leader
			go r.sendRPC(request, client, peer)
			r.Logger.Debug().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("leaderAddress", peer.Address).
				Str("leaderId", peer.ID).
				Str("action", action.String()).
				Msgf("Membership change request sent")
		}
		return
	}
	r.askForMembershipInProgress.Store(false)
}

// membershipChangeResponse will handle membership change request
// response from the leader by the new node.
// It will update the current node configuration
// and retry if needed
func (r *Rafty) membershipChangeResponse(resp RPCResponse) {
	response := resp.Response.(RPCMembershipChangeResponse)
	err := resp.Error
	targetPeer := resp.TargetPeer

	defer r.askForMembershipInProgress.Store(false)
	if r.getState() != Down && err != nil {
		r.Logger.Error().Err(err).
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("leaderAddress", targetPeer.Address).
			Str("leaderId", targetPeer.ID).
			Str("action", MembershipChange(response.ActionPerformed).String()).
			Msgf("Fail to ask for membership change request")
		return
	}

	r.Logger.Debug().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("leaderAddress", targetPeer.Address).
		Str("leaderId", targetPeer.ID).
		Str("action", MembershipChange(response.ActionPerformed).String()).
		Msgf("Membership change request response %t", response.Success)

	if response.Success {
		r.askForMembership.Store(false)
		r.Logger.Debug().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("leaderAddress", targetPeer.Address).
			Str("leaderId", targetPeer.ID).
			Str("action", MembershipChange(response.ActionPerformed).String()).
			Msgf("Membership change request successful")
	}
}

// sendMembershipChangeLeaveOnTerminate is used by the current node
// to ask leader to be remove itself of the cluster as it's going down.
// This is only used by nodes who are read replicas.
// Using it for nodes who are followers are not recommended.
func (r *Rafty) sendMembershipChangeLeaveOnTerminate() {
	rpcMembershipChangeChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType: MembershipChangeRequest,
		Request: RPCMembershipChangeRequest{
			Id:           r.id,
			Address:      r.Address.String(),
			Action:       uint32(LeaveOnTerminate),
			LastLogIndex: r.lastLogIndex.Load(),
			LastLogTerm:  r.lastLogTerm.Load(),
			ReadReplica:  r.options.ReadReplica,
		},
		Timeout:      time.Second,
		ResponseChan: rpcMembershipChangeChan,
	}

	leader := r.getLeader()
	if leader.address != "" && leader.id != "" {
		peer := peer{
			Address: leader.address,
			address: getNetAddress(leader.address),
			ID:      leader.id,
		}

		client := r.connectionManager.getClient(peer.Address, leader.id)
		if client != nil {
			// membership is a kind of a long running progress
			// so we need to send the request in background
			// to be able to receive calls from leader
			go r.sendRPC(request, client, peer)
			r.Logger.Debug().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("leaderAddress", peer.Address).
				Str("leaderId", peer.ID).
				Str("action", LeaveOnTerminate.String()).
				Msgf("Membership change request sent")
		}

		select {
		case <-time.After(5 * time.Second):
			return

		case resp := <-rpcMembershipChangeChan:
			response := resp.Response.(RPCMembershipChangeResponse)
			if response.Success {
				r.askForMembership.Store(false)
				r.Logger.Debug().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("leaderAddress", peer.Address).
					Str("leaderId", peer.ID).
					Str("action", LeaveOnTerminate.String()).
					Msgf("Membership change request successful")
			}
		}
	}
}

// rpcMembershipNotLeader is an rpc helper that allow to reploy to clients
// when membership is sent to a non leader node
func (r *Rafty) rpcMembershipNotLeader(data RPCRequest) {
	leader := r.getLeader()
	response := &raftypb.MembershipChangeResponse{LeaderId: leader.id, LeaderAddress: leader.address}
	rpcResponse := RPCResponse{
		Response: response,
		Error:    ErrNotLeader,
	}
	data.ResponseChan <- rpcResponse
}

// bootstrapCluster is used by the current node
// to bootstrap the cluster with all initial nodes.
// This should be only call once and on one node.
// If already bootstrapped, and error will be returned
func (r *Rafty) bootstrapCluster(data RPCRequest) {
	response := &raftypb.BootstrapClusterResponse{}
	rpcResponse := RPCResponse{}
	defer func() {
		data.ResponseChan <- rpcResponse
	}()
	if r.isBootstrapped.Load() {
		rpcResponse.Error = ErrClusterAlreadyBootstrapped
		rpcResponse.Response = response
		return
	}

	r.currentTerm.Add(1)
	peers, _ := r.getAllPeers()
	encodedPeers := encodePeers(peers)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      r.currentTerm.Load(),
			Command:   encodedPeers,
		},
	}
	_ = r.logs.appendEntries(entries, false)
	_ = r.logs.applyConfigEntry(entries[0])
	if r.options.PersistDataOnDisk {
		if err := r.storage.data.storeWithEntryIndex(int(entries[0].Index)); err != nil {
			r.Logger.Fatal().Err(err).Msg("Fail to persist data on disk")
		}
		if err := r.storage.metadata.store(); err != nil {
			r.Logger.Fatal().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("term", fmt.Sprintf("%d", r.currentTerm.Load())).
				Msgf("Fail to persist metadata")
		}
	}

	response.Success = true
	rpcResponse.Response = response
	r.nextIndex.Add(1)
	r.matchIndex.Add(1)
	r.lastApplied.Add(1)
	r.commitIndex.Add(1)
	r.isBootstrapped.Store(true)
	r.switchState(Leader, stepUp, true, r.currentTerm.Load())
	r.timer.Reset(r.randomElectionTimeout())
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Cluster successfully bootstrapped")
}
