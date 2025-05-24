package rafty

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestAskNodeID_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.AskNodeIDRequest{}
	_, err = rpcm.AskNodeID(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestGetLeader_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.GetLeaderRequest{}
	_, err = rpcm.GetLeader(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestSendPreVoteRequest_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.PreVoteRequest{}
	_, err = rpcm.SendPreVoteRequest(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestSendVoteRequest_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.VoteRequest{}
	_, err = rpcm.SendVoteRequest(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestSendAppendEntriesRequest_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.AppendEntryRequest{}
	_, err = rpcm.SendAppendEntriesRequest(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestClientGetLeader_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.ClientGetLeaderRequest{}
	_, err = rpcm.ClientGetLeader(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestForwardCommandToLeader_down(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	rpcm := rpcManager{rafty: s}
	request := &raftypb.ForwardCommandToLeaderRequest{}
	_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
	assert.Equal(ErrShutdown, err)
}

func TestForwardCommandToLeader_empty(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.State = Follower
	s.isRunning.Store(true)

	rpcm := rpcManager{rafty: s}
	i := 0
	command := Command{Kind: 99, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
	data, err := encodeCommand(command)
	assert.Nil(err)
	request := &raftypb.ForwardCommandToLeaderRequest{Command: data}
	response, err := rpcm.ForwardCommandToLeader(context.Background(), request)
	assert.Equal(nil, err)
	assert.Equal(&raftypb.ForwardCommandToLeaderResponse{}, response)
}
