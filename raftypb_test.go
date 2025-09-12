package rafty

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
)

func TestRaftypb_AskNodeID(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	rpcm := rpcManager{rafty: s}
	request := &raftypb.AskNodeIDRequest{}

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err := rpcm.AskNodeID(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err := rpcm.AskNodeID(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err := rpcm.AskNodeID(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_GetLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	rpcm := rpcManager{rafty: s}
	request := &raftypb.GetLeaderRequest{}

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err := rpcm.GetLeader(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err := rpcm.GetLeader(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err := rpcm.GetLeader(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_SendPreVoteRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.PreVoteRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendPreVoteRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcPreVoteRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.PreVoteResponse{PeerId: s.id, Granted: false, CurrentTerm: s.currentTerm.Load()},
			}
		}()

		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcPreVoteRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendPreVoteRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcPreVoteRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcPreVoteRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.SendPreVoteRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_SendVoteRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.VoteRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendVoteRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcVoteRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.VoteResponse{PeerId: s.id, Granted: false, CurrentTerm: s.currentTerm.Load()},
			}
		}()
		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcVoteRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendVoteRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcVoteRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcVoteRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.SendVoteRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_SendAppendEntriesRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.AppendEntryRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendAppendEntriesRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcAppendEntriesRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.AppendEntryResponse{Success: false},
			}
		}()
		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcAppendEntriesRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendAppendEntriesRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcAppendEntriesRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.SendAppendEntriesRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_ClientGetLeader(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	rpcm := rpcManager{rafty: s}
	request := &raftypb.ClientGetLeaderRequest{}

	t.Run("down", func(t *testing.T) {
		s.State = Down
		_, err := rpcm.ClientGetLeader(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		_, err := rpcm.ClientGetLeader(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("bootstrap_cluster", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		s.options.BootstrapCluster = true
		defer func() {
			s.options.BootstrapCluster = false
		}()
		_, err := rpcm.ClientGetLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("leader", func(t *testing.T) {
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		_, err := rpcm.ClientGetLeader(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_ForwardCommandToLeader(t *testing.T) {
	assert := assert.New(t)

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: 99, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("decode_command", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		buffer := new(bytes.Buffer)
		_ = binary.Write(buffer, binary.LittleEndian, uint32(1)) // Kind
		_ = binary.Write(buffer, binary.LittleEndian, uint64(3)) // KeyLen
		buffer.Write([]byte("abc"))                              // Key
		_ = binary.Write(buffer, binary.LittleEndian, uint64(2)) // ValueLen
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("bootstrap_cluster", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		s.options.BootstrapCluster = true
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: 99, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Equal(ErrClusterNotBootstrapped, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.ForwardCommandToLeader(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		go func() {
			data := <-s.rpcForwardCommandToLeaderRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.ForwardCommandToLeaderResponse{},
			}
		}()

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcForwardCommandToLeaderRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.ForwardCommandToLeader(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		go func() {
			<-s.rpcForwardCommandToLeaderRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		go func() {
			<-s.rpcForwardCommandToLeaderRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Error(err)
	})

	t.Run("fake_command", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		i := 0
		command := Command{Kind: 99, Key: fmt.Sprintf("key%s%d", s.id, i), Value: fmt.Sprintf("value%d", i)}
		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(command, buffer))
		request := &raftypb.ForwardCommandToLeaderRequest{Command: buffer.Bytes()}

		_, err := rpcm.ForwardCommandToLeader(context.Background(), request)
		assert.Nil(err)
	})
}

func TestRaftypb_SendTimeoutNowRequest(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()

	t.Run("up", func(t *testing.T) {
		s.State = Follower
		s.isRunning.Store(true)
		rpcm := rpcManager{rafty: s}
		request := &raftypb.TimeoutNowRequest{}
		response, err := rpcm.SendTimeoutNowRequest(context.Background(), request)
		assert.Equal(nil, err)
		assert.Equal(true, response.Success)
	})
}

func TestRaftypb_SendMembershipChangeRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.MembershipChangeRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("bootstrap_cluster", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		s.options.BootstrapCluster = true
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Equal(ErrClusterNotBootstrapped, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendMembershipChangeRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcMembershipChangeRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.MembershipChangeResponse{},
			}
		}()

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcMembershipChangeRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendMembershipChangeRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcMembershipChangeRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcMembershipChangeRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.SendMembershipChangeRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_SendBootstrapClusterRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.BootstrapClusterRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendBootstrapClusterRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcBootstrapClusterRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.BootstrapClusterResponse{},
			}
		}()

		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcBootstrapClusterRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendBootstrapClusterRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcBootstrapClusterRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcBootstrapClusterRequestChan
			time.Sleep(time.Second)
		}()

		_, err := rpcm.SendBootstrapClusterRequest(context.Background(), request)
		assert.Error(err)
	})
}

func TestRaftypb_SendInstallSnapshotRequest(t *testing.T) {
	assert := assert.New(t)
	request := &raftypb.InstallSnapshotRequest{}

	t.Run("down", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.State = Down
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Equal(ErrShutdown, err)
	})

	t.Run("context_done_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendInstallSnapshotRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()
		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("up", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			data := <-s.rpcInstallSnapshotRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.InstallSnapshotResponse{},
			}
		}()

		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Nil(err)
	})

	t.Run("context_done_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			<-s.rpcInstallSnapshotRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		_, err := rpcm.SendInstallSnapshotRequest(ctx, request)
		assert.Error(err)
	})

	t.Run("quit_context_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcInstallSnapshotRequestChan
		}()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Error(err)
	})

	t.Run("timeout_second", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.State = Follower
		rpcm := rpcManager{rafty: s}

		go func() {
			<-s.rpcInstallSnapshotRequestChan
			time.Sleep(5 * time.Second)
		}()

		_, err := rpcm.SendInstallSnapshotRequest(context.Background(), request)
		assert.Error(err)
	})
}
