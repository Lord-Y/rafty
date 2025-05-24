package rafty

import (
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestRpcs_Fake(t *testing.T) {
	assert := assert.New(t)
	r := basicNodeSetup()

	request := RPCRequest{
		RPCType: 99,
		Request: RPCAskNodeIDRequest{
			Id:      r.id,
			Address: r.Address.String(),
		},
		Timeout:      time.Second,
		ResponseChan: r.rpcAskNodeIDChan,
	}

	go r.sendRPC(request, nil, peer{})
	data := <-r.rpcAskNodeIDChan
	assert.Error(data.Error)
}

func TestMakeRPCVoteResponse_Nil(t *testing.T) {
	assert := assert.New(t)

	data := &raftypb.VoteResponse{}
	result := makeRPCVoteResponse(data, 0)
	assert.Equal(RPCVoteResponse{}, result)
}
