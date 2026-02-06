package rafty

import (
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRpcs_Fake(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()

	request := RPCRequest{
		RPCType: 99,
		Request: RPCAskNodeIDRequest{
			Id:      s.id,
			Address: s.Address.String(),
		},
		Timeout:      time.Second,
		ResponseChan: s.rpcAskNodeIDChan,
	}

	go s.sendRPC(request, nil, Peer{})
	data := <-s.rpcAskNodeIDChan
	assert.Error(data.Error)
}

func TestRpcs_MakeRPCVoteResponse_Nil(t *testing.T) {
	assert := assert.New(t)

	data := &raftypb.VoteResponse{}
	result := makeRPCVoteResponse(data, 0)
	assert.Equal(RPCVoteResponse{}, result)
}

func TestRpcs_askNodeIDResult(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.State = Follower

	s.configuration.ServerMembers[0].ID = "xx"
	rpcResponse := RPCAskNodeIDResponse{
		LeaderID:      "yy",
		LeaderAddress: s.configuration.ServerMembers[1].Address,
	}
	resp := RPCResponse{
		TargetPeer: Peer{Address: s.configuration.ServerMembers[0].Address},
		Response:   rpcResponse,
		Error:      ErrTimeout,
	}

	t.Run("error", func(t *testing.T) {
		s.askNodeIDResult(resp)
	})

	t.Run("empty_id", func(t *testing.T) {
		resp.Error = nil
		s.askNodeIDResult(resp)
	})
	t.Run("leader", func(t *testing.T) {
		resp.Error = nil
		s.configuration.ServerMembers[1].ID = "yy"
		s.askNodeIDResult(resp)
	})

	t.Run("askForMembership", func(t *testing.T) {
		resp.Error = nil
		rpcResponse.AskForMembership = true
		resp.Response = rpcResponse
		s.configuration.ServerMembers[1].ID = "yy"
		s.askNodeIDResult(resp)
	})
}

func TestRpcs_makeRPCGetLeaderRequest_Nil(t *testing.T) {
	assert := assert.New(t)

	result := makeRPCGetLeaderResponse(nil, 0)
	assert.Equal(RPCGetLeaderResponse{}, result)
}

func TestRpcs_makeRPCGetLeaderRequest(t *testing.T) {
	assert := assert.New(t)

	request := RPCGetLeaderRequest{
		PeerID:      "n1",
		PeerAddress: "127.0.0.1:50051",
		TotalPeers:  3,
	}
	result := makeRPCGetLeaderRequest(request)
	assert.Equal("n1", result.PeerId)
	assert.Equal("127.0.0.1:50051", result.PeerAddress)
}

func TestRpcs_makeRPCGetLeaderResponse(t *testing.T) {
	assert := assert.New(t)

	resp := &raftypb.GetLeaderResponse{
		LeaderId:         "52",
		LeaderAddress:    "127.0.0.2:50052",
		PeerId:           "51",
		AskForMembership: true,
	}
	result := makeRPCGetLeaderResponse(resp, 3)
	assert.Equal("52", result.LeaderID)
	assert.Equal("127.0.0.2:50052", result.LeaderAddress)
	assert.Equal("51", result.PeerID)
	assert.Equal(3, result.TotalPeers)
	assert.True(result.AskForMembership)
}

func TestRpcs_getLeaderResult(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.State = Follower

	rpcResponse := RPCGetLeaderResponse{}
	resp := RPCResponse{
		TargetPeer: Peer{Address: s.configuration.ServerMembers[0].Address},
		Response:   rpcResponse,
		Error:      ErrTimeout,
	}

	t.Run("error", func(t *testing.T) {
		s.getLeaderResult(resp)
	})

	t.Run("no_leader", func(t *testing.T) {
		resp.Error = nil
		rpcResponse.TotalPeers = 2
		s.leaderCount.Store(1)
		defer s.leaderCount.Store(uint64(0))
		resp.Response = rpcResponse
		s.getLeaderResult(resp)
	})

	t.Run("leader_found", func(t *testing.T) {
		resp.Error = nil
		s.leaderFound.Store(true)
		defer s.leaderFound.Store(false)
		s.getLeaderResult(resp)
	})

	t.Run("leader_id", func(t *testing.T) {
		rpcResponse.LeaderID = "xxx"
		rpcResponse.LeaderAddress = "127.0.0.1:60000"
		resp.Error = nil
		resp.Response = rpcResponse
		s.getLeaderResult(resp)
	})
}

func TestRpcs_sendGetLeaderRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()

	t.Run("follower", func(t *testing.T) {
		s.State = Follower
		s.leaderFound.Store(true)
		s.leaderCount.Store(42)

		s.sendGetLeaderRequest()
		time.Sleep(20 * time.Millisecond)

		assert.False(s.leaderFound.Load())
		assert.Equal(uint64(42), s.leaderCount.Load())
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		s.sendGetLeaderRequest()
		time.Sleep(20 * time.Millisecond)
		assert.Equal(uint64(42), s.leaderCount.Load())
	})
}

func TestRpcs_sendRPC_GetLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()

	mockClient := new(MockRaftyClientTestify)
	mockClient.On("GetLeader", mock.Anything, mock.AnythingOfType("*raftypb.GetLeaderRequest"), mock.Anything).
		Return(&raftypb.GetLeaderResponse{
			LeaderId:      "52",
			LeaderAddress: "127.0.0.2:50052",
			PeerId:        "52",
		}, nil).Once()

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType: GetLeader,
		Request: RPCGetLeaderRequest{
			PeerID:      "51",
			PeerAddress: "127.0.0.1:50051",
			TotalPeers:  3,
		},
		Timeout:      time.Second,
		ResponseChan: responseChan,
	}

	target := Peer{ID: "52", Address: "127.0.0.2:50052"}
	s.sendRPC(request, mockClient, target)
	data := <-responseChan
	assert.NoError(data.Error)
	assert.Equal(target, data.TargetPeer)
	response := data.Response.(RPCGetLeaderResponse)
	assert.Equal("52", response.LeaderID)
	assert.Equal("127.0.0.2:50052", response.LeaderAddress)
	assert.Equal(3, response.TotalPeers)
	mockClient.AssertExpectations(t)
}

func TestRpcs_bootstrapCluster(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.State = Follower
	s.options.BootstrapCluster = true

	responseChan := make(chan RPCResponse, 1)
	request := RPCRequest{
		RPCType:      BootstrapClusterRequest,
		ResponseChan: responseChan,
	}

	t.Run("bootstrap_cluster", func(t *testing.T) {
		s.bootstrapCluster(request)
		data := <-responseChan
		response := data.Response.(*raftypb.BootstrapClusterResponse)
		assert.Equal(true, response.Success)
	})

	t.Run("bootstrap_cluster_error", func(t *testing.T) {
		s.bootstrapCluster(request)
		data := <-responseChan
		assert.Error(ErrClusterAlreadyBootstrapped, data.Error)
	})
}
