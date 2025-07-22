package rafty

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestRaftypb_AskNodeID(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.AskNodeIDRequest{}
	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.AskNodeID(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.AskNodeID(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err = rpcm.AskNodeID(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_GetLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.GetLeaderRequest{}
	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.GetLeader(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.GetLeader(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err = rpcm.GetLeader(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_SendPreVoteRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.PreVoteRequest{}
	t.Run("timeout", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.SendPreVoteRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_context", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer func() {
			s.stopCtx()
			s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}()
		_, err = rpcm.SendPreVoteRequest(s.quitCtx, request)
		assert.NotNil(err)
	})

	t.Run("up", func(t *testing.T) {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			data := <-s.rpcPreVoteRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.PreVoteResponse{PeerID: s.id, Granted: false, CurrentTerm: s.currentTerm.Load()},
			}
		}()
		_, err = rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("timeout_second", func(t *testing.T) {
		s.State = Follower
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcPreVoteRequestChan
			time.Sleep(time.Second)
		}()
		_, err = rpcm.SendPreVoteRequest(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})
}

func TestRaftypb_SendVoteRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.VoteRequest{}
	t.Run("timeout", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.SendVoteRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_context", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer func() {
			s.stopCtx()
			s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}()
		_, err = rpcm.SendVoteRequest(s.quitCtx, request)
		assert.NotNil(err)
	})

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			data := <-s.rpcVoteRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.VoteResponse{PeerID: s.id, Granted: false, CurrentTerm: s.currentTerm.Load()},
			}
		}()
		_, err = rpcm.SendVoteRequest(context.Background(), request)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("timeout_second", func(t *testing.T) {
		s.State = Follower
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcVoteRequestChan
			time.Sleep(time.Second)
		}()
		_, err = rpcm.SendVoteRequest(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.SendVoteRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})
}

func TestRaftypb_SendAppendEntriesRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.AppendEntryRequest{}
	t.Run("timeout", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_context", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer func() {
			s.stopCtx()
			s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}()
		_, err = rpcm.SendAppendEntriesRequest(s.quitCtx, request)
		assert.NotNil(err)
	})

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			data := <-s.rpcAppendEntriesRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.AppendEntryResponse{Success: false},
			}
		}()
		_, err = rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("timeout_second", func(t *testing.T) {
		s.State = Follower
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(time.Second)
		}()
		_, err = rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})
}

func TestRaftypb_ClientGetLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.ClientGetLeaderRequest{}
	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.ClientGetLeader(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.ClientGetLeader(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("bootstrap_cluster", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		s.options.BootstrapCluster = true
		defer func() {
			s.options.BootstrapCluster = false
		}()
		_, err = rpcm.ClientGetLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err = rpcm.ClientGetLeader(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_ForwardCommandToLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	i := 0
	s.State = Follower
	s.isRunning.Store(true)
	rpcm := rpcManager{rafty: s}
	command := Command{Kind: 99, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
	buffer := new(bytes.Buffer)
	err = encodeCommand(command, buffer)
	assert.Nil(err)

	t.Run("up_command_fake", func(t *testing.T) {
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Equal(nil, err)
	})

	t.Run("up_command_set", func(t *testing.T) {
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		err := encodeCommand(command, buffer)
		assert.Nil(err)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			data := <-s.rpcForwardCommandToLeaderRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.ForwardCommandToLeaderResponse{},
			}
		}()

		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("up_command_bootstrap_cluster", func(t *testing.T) {
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		err := encodeCommand(command, buffer)
		assert.Nil(err)
		s.options.BootstrapCluster = true
		defer func() {
			s.options.BootstrapCluster = false
		}()

		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
		s.wg.Wait()
	})

	t.Run("timeout_second_sending", func(t *testing.T) {
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		err = encodeCommand(command, buffer)
		assert.Nil(err)

		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_second_response", func(t *testing.T) {
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		err = encodeCommand(command, buffer)
		assert.Nil(err)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcForwardCommandToLeaderRequestChan
			time.Sleep(time.Second)
		}()

		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		rpcm := rpcManager{rafty: s}
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}
		_, err = rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.NotNil(err)
	})
}

func TestRaftypb_SendTimeoutNowRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		rpcm := rpcManager{rafty: s}
		assert.Nil(err)
		request := &raftypb.TimeoutNowRequest{}
		response, err := rpcm.SendTimeoutNowRequest(context.Background(), request)
		assert.Equal(nil, err)
		assert.Equal(true, response.Success)
	})
}

func TestRaftypb_SendMembershipChangeRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.MembershipChangeRequest{Id: "newnode", Address: "127.0.0.1:60000", Action: uint32(Add)}

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout", func(t *testing.T) {
		s.State = Leader
		s.isRunning.Store(true)
		_, err = rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_context", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer func() {
			s.stopCtx()
			s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}()
		_, err = rpcm.SendMembershipChangeRequest(s.quitCtx, request)
		assert.NotNil(err)
	})

	t.Run("timeout_second_response", func(t *testing.T) {
		rpcm := rpcManager{rafty: s}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcMembershipChangeRequestChan
			time.Sleep(time.Second)
		}()

		_, err = rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("up_timeout", func(t *testing.T) {
		rpcm := rpcManager{rafty: s}
		_, err = rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_SendBootstrapClusterRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	err := s.parsePeers()
	assert.Nil(err)
	s.options.BootstrapCluster = true

	s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	rpcm := rpcManager{rafty: s}
	request := &raftypb.BootstrapClusterRequest{}

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err = rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err = rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.NotNil(err)
	})

	t.Run("timeout_context", func(t *testing.T) {
		s.quitCtx, s.stopCtx = context.WithTimeout(context.Background(), time.Millisecond)
		defer func() {
			s.stopCtx()
			s.quitCtx, s.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}()
		_, err = rpcm.SendBootstrapClusterRequest(s.quitCtx, request)
		assert.NotNil(err)
	})

	t.Run("timeout_second_response", func(t *testing.T) {
		rpcm := rpcManager{rafty: s}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-s.rpcBootstrapClusterRequestChan
			time.Sleep(time.Second)
		}()

		_, err = rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.NotNil(err)
		s.wg.Wait()
	})

	t.Run("up_timeout", func(t *testing.T) {
		rpcm := rpcManager{rafty: s}
		_, err = rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Error(err)
	})
}
