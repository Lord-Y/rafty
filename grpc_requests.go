package rafty

import (
	"context"

	"github.com/Lord-Y/rafty/grpcrequests"
)

func (rpc *rpcManager) SayHello(ctx context.Context, in *grpcrequests.HelloRequest) (*grpcrequests.HelloReply, error) {
	rpc.rafty.Logger.Info().Msgf("Received: %s", in.GetName())
	return &grpcrequests.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (rpc *rpcManager) SendPreVoteRequest(ctx context.Context, in *grpcrequests.PreVoteRequest) (*grpcrequests.PreVoteResponse, error) {
	var s struct{}
	rpc.rafty.rpcPreVoteRequestChanReader <- s
	response := <-rpc.rafty.rpcPreVoteRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) SendVoteRequest(ctx context.Context, in *grpcrequests.VoteRequest) (*grpcrequests.VoteResponse, error) {
	rpc.rafty.rpcSendVoteRequestChanReader <- in
	response := <-rpc.rafty.rpcSendVoteRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) GetLeader(ctx context.Context, in *grpcrequests.GetLeaderRequest) (*grpcrequests.GetLeaderResponse, error) {
	rpc.rafty.rpcGetLeaderChanReader <- in
	response := <-rpc.rafty.rpcGetLeaderChanWritter
	return response, nil
}

func (rpc *rpcManager) SetLeader(ctx context.Context, in *grpcrequests.SetLeaderRequest) (*grpcrequests.SetLeaderResponse, error) {
	rpc.rafty.rpcSetLeaderChanReader <- in
	response := <-rpc.rafty.rpcSetLeaderChanWritter
	return response, nil
}

func (rpc *rpcManager) AskNodeID(ctx context.Context, in *grpcrequests.AskNodeIDRequest) (*grpcrequests.AskNodeIDResponse, error) {
	return &grpcrequests.AskNodeIDResponse{PeerID: rpc.rafty.ID}, nil
}

func (rpc *rpcManager) SendHeartbeats(ctx context.Context, in *grpcrequests.SendHeartbeatRequest) (*grpcrequests.SendHeartbeatResponse, error) {
	rpc.rafty.rpcSendHeartbeatsChanReader <- in
	response := <-rpc.rafty.rpcSendHeartbeatsChanWritter
	return response, nil
}

func (rpc *rpcManager) SendAppendEntriesRequest(ctx context.Context, in *grpcrequests.SendAppendEntryRequest) (*grpcrequests.SendAppendEntryResponse, error) {
	rpc.rafty.rpcSendAppendEntriesRequestChanReader <- in
	response := <-rpc.rafty.rpcSendAppendEntriesRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) ClientGetLeader(ctx context.Context, in *grpcrequests.ClientGetLeaderRequest) (*grpcrequests.ClientGetLeaderResponse, error) {
	rpc.rafty.rpcClientGetLeaderChanReader <- in
	response := <-rpc.rafty.rpcClientGetLeaderChanWritter
	return response, nil
}
