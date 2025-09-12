package rafty

import (
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestRpcs_Fake(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
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
		assert.Nil(os.RemoveAll(s.options.DataDir))
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
		Error:      ErrTimeoutSendingRequest,
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

func TestRpcs_getLeaderResult(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	s.State = Follower

	rpcResponse := RPCGetLeaderResponse{}
	resp := RPCResponse{
		TargetPeer: Peer{Address: s.configuration.ServerMembers[0].Address},
		Response:   rpcResponse,
		Error:      ErrTimeoutSendingRequest,
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

func TestRpcs_membershipChangeResponse(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	s.State = Follower

	rpcResponse := RPCMembershipChangeResponse{}
	resp := RPCResponse{
		TargetPeer: Peer{Address: s.configuration.ServerMembers[0].Address},
		Response:   rpcResponse,
		Error:      ErrTimeoutSendingRequest,
	}

	t.Run("error", func(t *testing.T) {
		s.membershipChangeResponse(resp)
	})

	t.Run("success", func(t *testing.T) {
		resp.Error = nil
		rpcResponse.Success = true
		resp.Response = rpcResponse
		s.membershipChangeResponse(resp)
	})
}

func TestRpcs_bootstrapCluster(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
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
