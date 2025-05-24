package rafty

import (
	"context"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
)

// RPCType is used to build rpc requests
type RPCType uint16

const (
	// AskNodeID will be used to ask node id
	AskNodeID RPCType = iota

	// GetLeader will be used to ask who is the leader
	GetLeader

	// PreVoteRequest is used during PreVote
	PreVoteRequest

	// VoteRequest is used during PreVote
	VoteRequest
)

type RPCRequest struct {
	RPCType      RPCType
	Request      any
	Timeout      time.Duration
	ResponseChan chan<- RPCResponse
}

type RPCResponse struct {
	TargetPeer peer
	Response   any
	Error      error
}

type RPCAskNodeIDRequest struct {
	Id, Address string
}

type RPCAskNodeIDResponse struct {
	LeaderID, LeaderAddress, PeerID string
	ReadOnlyNode                    bool
}

type RPCGetLeaderRequest struct {
	PeerID, PeerAddress string
	TotalPeers          int
}

type RPCGetLeaderResponse struct {
	LeaderID, LeaderAddress, PeerID string
	TotalPeers                      int
}

type RPCPreVoteRequest struct {
	Id          string
	CurrentTerm uint64
}

type RPCPreVoteResponse struct {
	PeerID      string
	CurrentTerm uint64
	Granted     bool
}

type RPCVoteRequest struct {
	CandidateId, CandidateAddress          string
	CurrentTerm, LastLogIndex, LastLogTerm uint64
}

type RPCVoteResponse struct {
	PeerID                     string
	RequesterTerm, CurrentTerm uint64
	Granted                    bool
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
		resp, err := client.SendPreVoteRequest(
			ctx,
			makeRPCPreVoteRequest(request.Request.(RPCPreVoteRequest)),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCPreVoteResponse(resp), Error: err, TargetPeer: peer}

	case VoteRequest:
		req := request.Request.(RPCVoteRequest)
		resp, err := client.SendVoteRequest(
			ctx,
			makeRPCVoteRequest(req),
			options...,
		)
		request.ResponseChan <- RPCResponse{Response: makeRPCVoteResponse(resp, req.CurrentTerm), Error: err, TargetPeer: peer}

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
		LeaderID:      data.LeaderID,
		LeaderAddress: data.LeaderAddress,
		PeerID:        data.PeerID,
		ReadOnlyNode:  data.ReadOnlyNode,
	}
}

// makeRPCGetLeaderRequest build leader request
func makeRPCGetLeaderRequest(data RPCGetLeaderRequest) *raftypb.GetLeaderRequest {
	return &raftypb.GetLeaderRequest{
		PeerID:      data.PeerID,
		PeerAddress: data.PeerAddress,
	}
}

// makeRPCGetLeaderResponse build leader response
func makeRPCGetLeaderResponse(data *raftypb.GetLeaderResponse, totalPeers int) RPCGetLeaderResponse {
	if data == nil {
		return RPCGetLeaderResponse{}
	}
	return RPCGetLeaderResponse{
		LeaderID:      data.LeaderID,
		LeaderAddress: data.LeaderAddress,
		PeerID:        data.PeerID,
		TotalPeers:    totalPeers,
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
func makeRPCPreVoteResponse(data *raftypb.PreVoteResponse) RPCPreVoteResponse {
	if data == nil {
		return RPCPreVoteResponse{}
	}
	return RPCPreVoteResponse{
		CurrentTerm: data.CurrentTerm,
		Granted:     data.Granted,
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
		return RPCVoteResponse{}
	}
	return RPCVoteResponse{
		PeerID:        data.PeerID,
		RequesterTerm: term,
		CurrentTerm:   data.CurrentTerm,
		Granted:       data.Granted,
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
			if client != nil && !r.leaderLost.Load() && r.getState() != Down {
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("peerAddress", peer.address.String()).
					Msgf("Try to fetch peer id")

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
		r.configuration.ServerMembers[index].ReadOnlyNode = response.ReadOnlyNode
	}
	peers := r.configuration.ServerMembers
	r.mu.Unlock()

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

	if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !response.ReadOnlyNode {
		r.clusterSizeCounter.Add(1)
	}

	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Str("peerAddress", targetPeer.address.String()).
		Str("peerId", response.PeerID).
		Msgf("Peer id fetched")
}

// sendGetLeaderRequest allow the current node
// ask to other nodes who is the actual leader
// and prevent starting election campain
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
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("peerAddress", peer.address.String()).
					Str("peerId", peer.ID).
					Msgf("Ask who is the leader")

				r.sendRPC(request, client, peer)
			}
		}()
	}
}

// getLeaderResult allow the current node
// ask to other nodes who is the actual leader
// and prevent starting election campain
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
		r.leaderLost.Store(false)

		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Str("leaderAddress", response.LeaderAddress).
			Str("leaderId", response.LeaderID).
			Msgf("Leader found")
		r.leaderLastContactDate.Store(time.Now())
	}

	if !r.leaderFound.Load() && int(r.leaderCount.Load()) == response.TotalPeers {
		r.leaderLost.Store(true)
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("There is no leader")
	}
}
