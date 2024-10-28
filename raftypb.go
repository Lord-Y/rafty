package rafty

import (
	"context"

	"github.com/Lord-Y/rafty/raftypb"
)

func (rpc *rpcManager) GetLeader(ctx context.Context, in *raftypb.GetLeaderRequest) (*raftypb.GetLeaderResponse, error) {
	rpc.rafty.rpcGetLeaderChanReader <- in
	response := <-rpc.rafty.rpcGetLeaderChanWritter
	return response, nil
}

func (rpc *rpcManager) SendPreVoteRequest(ctx context.Context, in *raftypb.PreVoteRequest) (*raftypb.PreVoteResponse, error) {
	var s struct{}
	rpc.rafty.rpcPreVoteRequestChanReader <- s
	response := <-rpc.rafty.rpcPreVoteRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) SendVoteRequest(ctx context.Context, in *raftypb.VoteRequest) (*raftypb.VoteResponse, error) {
	rpc.rafty.rpcSendVoteRequestChanReader <- in
	response := <-rpc.rafty.rpcSendVoteRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) SetLeader(ctx context.Context, in *raftypb.SetLeaderRequest) (*raftypb.SetLeaderResponse, error) {
	rpc.rafty.rpcSetLeaderChanReader <- in
	response := <-rpc.rafty.rpcSetLeaderChanWritter
	return response, nil
}

func (rpc *rpcManager) AskNodeID(ctx context.Context, in *raftypb.AskNodeIDRequest) (*raftypb.AskNodeIDResponse, error) {
	return &raftypb.AskNodeIDResponse{PeerID: rpc.rafty.ID}, nil
}

func (rpc *rpcManager) SendAppendEntriesRequest(ctx context.Context, in *raftypb.AppendEntryRequest) (*raftypb.AppendEntryResponse, error) {
	rpc.rafty.rpcSendAppendEntriesRequestChanReader <- in
	response := <-rpc.rafty.rpcSendAppendEntriesRequestChanWritter
	return response, nil
}

func (rpc *rpcManager) ClientGetLeader(ctx context.Context, in *raftypb.ClientGetLeaderRequest) (*raftypb.ClientGetLeaderResponse, error) {
	rpc.rafty.rpcClientGetLeaderChanReader <- in
	response := <-rpc.rafty.rpcClientGetLeaderChanWritter
	return response, nil
}

func (r *rpcManager) ForwardCommandToLeader(ctx context.Context, reader *raftypb.ForwardCommandToLeaderRequest) (*raftypb.ForwardCommandToLeaderResponse, error) {
	cmd := r.rafty.decodeCommand(reader.Command)
	if cmd.kind == commandSet {
		r.rafty.rpcForwardCommandToLeaderRequestChanReader <- &raftypb.ForwardCommandToLeaderRequest{Command: reader.Command}

		response := <-r.rafty.rpcForwardCommandToLeaderRequestChanWritter
		return &raftypb.ForwardCommandToLeaderResponse{Data: response.Data, Error: response.Error}, nil
	}
	return &raftypb.ForwardCommandToLeaderResponse{}, nil
}
