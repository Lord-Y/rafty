package rafty

import (
	"context"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockRaftyClientTestify struct {
	mock.Mock
}

func (m *MockRaftyClientTestify) SendPreVoteRequest(ctx context.Context, req *raftypb.PreVoteRequest, opts ...grpc.CallOption) (*raftypb.PreVoteResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.PreVoteResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendVoteRequest(ctx context.Context, req *raftypb.VoteRequest, opts ...grpc.CallOption) (*raftypb.VoteResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.VoteResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) ClientGetLeader(ctx context.Context, req *raftypb.ClientGetLeaderRequest, opts ...grpc.CallOption) (*raftypb.ClientGetLeaderResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.ClientGetLeaderResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) GetLeader(ctx context.Context, req *raftypb.GetLeaderRequest, opts ...grpc.CallOption) (*raftypb.GetLeaderResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.GetLeaderResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendAppendEntriesRequest(ctx context.Context, req *raftypb.AppendEntryRequest, opts ...grpc.CallOption) (*raftypb.AppendEntryResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.AppendEntryResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) AskNodeID(ctx context.Context, req *raftypb.AskNodeIDRequest, opts ...grpc.CallOption) (*raftypb.AskNodeIDResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.AskNodeIDResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) ForwardCommandToLeader(ctx context.Context, req *raftypb.ForwardCommandToLeaderRequest, opts ...grpc.CallOption) (*raftypb.ForwardCommandToLeaderResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.ForwardCommandToLeaderResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendTimeoutNowRequest(ctx context.Context, req *raftypb.TimeoutNowRequest, opts ...grpc.CallOption) (*raftypb.TimeoutNowResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.TimeoutNowResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendMembershipChangeRequest(ctx context.Context, req *raftypb.MembershipChangeRequest, opts ...grpc.CallOption) (*raftypb.MembershipChangeResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.MembershipChangeResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendBootstrapClusterRequest(ctx context.Context, req *raftypb.BootstrapClusterRequest, opts ...grpc.CallOption) (*raftypb.BootstrapClusterResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.BootstrapClusterResponse)
	return resp, args.Error(1)
}

func (m *MockRaftyClientTestify) SendInstallSnapshotRequest(ctx context.Context, req *raftypb.InstallSnapshotRequest, opts ...grpc.CallOption) (*raftypb.InstallSnapshotResponse, error) {
	args := m.Called(ctx, req, opts)
	resp, _ := args.Get(0).(*raftypb.InstallSnapshotResponse)
	return resp, args.Error(1)
}
