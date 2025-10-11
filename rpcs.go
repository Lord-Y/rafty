package rafty

import (
	"context"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
)

// sendRPC is use to send rpc request
func (r *Rafty) sendRPC(request RPCRequest, client raftypb.RaftyClient, peer Peer) {
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

	default:
		request.ResponseChan <- RPCResponse{Error: ErrUnkownRPCType, TargetPeer: peer}
	}
}

// makeRPCAskNodeIDRequest build ask node id request
func makeRPCAskNodeIDRequest(data RPCAskNodeIDRequest) *raftypb.AskNodeIDRequest {
	return &raftypb.AskNodeIDRequest{
		Id:      data.Id,
		Address: data.Address,
		IsVoter: data.IsVoter,
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
		IsVoter:          data.IsVoter,
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

// sendAskNodeIDRequest will ask targeted node its id
func (r *Rafty) sendAskNodeIDRequest() {
	peers, _ := r.getPeers()

	request := RPCRequest{
		RPCType: AskNodeID,
		Request: RPCAskNodeIDRequest{
			Id:      r.id,
			Address: r.Address.String(),
			IsVoter: r.options.IsVoter,
		},
		Timeout:      time.Second,
		ResponseChan: r.rpcAskNodeIDChan,
	}

	for _, peer := range peers {
		go func() {
			if client := r.connectionManager.getClient(peer.address.String()); client != nil && r.getState() != Down {
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
	if index := slices.IndexFunc(r.configuration.ServerMembers, func(peer Peer) bool {
		return peer.address.String() == targetPeer.address.String() && peer.ID == ""
	}); index != -1 {
		r.configuration.ServerMembers[index].ID = response.PeerID
		r.configuration.ServerMembers[index].IsVoter = response.IsVoter
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

	if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && response.IsVoter {
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
			if client := r.connectionManager.getClient(peer.address.String()); client != nil && r.getState() != Down {
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
	encodedPeers := EncodePeers(peers)
	entry := makeNewLogEntry(r.currentTerm.Load(), LogConfiguration, encodedPeers)
	logs := []*LogEntry{entry}
	r.storeLogs(logs)

	_ = r.applyConfigEntry(makeProtobufLogEntry(entry)[0])
	if err := r.clusterStore.StoreMetadata(r.buildMetadata()); err != nil {
		panic(err)
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
