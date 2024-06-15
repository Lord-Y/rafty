package rafty

import (
	"context"
	"log"

	"github.com/Lord-Y/rafty/grpcrequests"
)

func (g *ProtobufSVC) SayHello(ctx context.Context, in *grpcrequests.HelloRequest) (*grpcrequests.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &grpcrequests.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (g *ProtobufSVC) SendPreVoteRequest(ctx context.Context, in *grpcrequests.PreVoteRequest) (*grpcrequests.PreVoteResponse, error) {
	var s struct{}
	g.rafty.rpcPreVoteRequestChanReader <- s
	response := <-g.rafty.rpcPreVoteRequestChanWritter
	return response, nil
}

func (g *ProtobufSVC) SendVoteRequest(ctx context.Context, in *grpcrequests.VoteRequest) (*grpcrequests.VoteResponse, error) {
	g.rafty.rpcSendVoteRequestChanReader <- in
	response := <-g.rafty.rpcSendVoteRequestChanWritter
	return response, nil
}

func (g *ProtobufSVC) GetLeader(ctx context.Context, in *grpcrequests.GetLeaderRequest) (*grpcrequests.GetLeaderResponse, error) {
	g.rafty.rpcGetLeaderChanReader <- in
	response := <-g.rafty.rpcGetLeaderChanWritter
	return response, nil
}

func (g *ProtobufSVC) SetLeader(ctx context.Context, in *grpcrequests.SetLeaderRequest) (*grpcrequests.SetLeaderResponse, error) {
	g.rafty.rpcSetLeaderChanReader <- in
	response := <-g.rafty.rpcSetLeaderChanWritter
	return response, nil
}

func (g *ProtobufSVC) SendHeartbeats(ctx context.Context, in *grpcrequests.SendHeartbeatRequest) (*grpcrequests.SendHeartbeatResponse, error) {
	g.rafty.rpcSendHeartbeatsChanReader <- in
	response := <-g.rafty.rpcSendHeartbeatsChanWritter
	return response, nil
}

func (g *ProtobufSVC) SendAppendEntriesRequest(ctx context.Context, in *grpcrequests.SendAppendEntryRequest) (*grpcrequests.SendAppendEntryResponse, error) {
	g.rafty.rpcSendAppendEntriesRequestChanReader <- in
	response := <-g.rafty.rpcSendAppendEntriesRequestChanWritter
	return response, nil
}
