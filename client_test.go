package rafty

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClient_submitCommand(t *testing.T) {
	t.Run("fake_command", func(t *testing.T) {
		cc := clusterConfig{
			t:           t,
			testName:    "3_nodes_client_fake_command",
			clusterSize: 3,
			// runTestInParallel: true,
			portStartRange: 36000,
		}
		cc.assert = assert.New(t)
		cc.testClustering(t)
	})

	t.Run("bootstrap", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.options.BootstrapCluster = true

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer))
		_, err := s.SubmitCommand(0, LogReplication, buffer.Bytes())
		assert.ErrorIs(err, ErrClusterNotBootstrapped)
	})

	t.Run("no_leader", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer))
		_, err := s.SubmitCommand(0, LogReplication, buffer.Bytes())
		assert.ErrorIs(err, ErrNoLeader)
	})

	t.Run("read_leader_lease", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.SubmitCommand(0, LogCommandReadLeaderLease, buffer.Bytes())
		assert.Error(err)
	})

	t.Run("read_linearize_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.SubmitCommand(0, LogCommandLinearizableRead, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeout)
	})

	t.Run("read_linearize_response", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			time.Sleep(100 * time.Millisecond)
			data := <-s.rpcAppendEntriesRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.ForwardCommandToLeaderResponse{
					Data: []byte("ok"),
				},
			}
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		result, err := s.SubmitCommand(0, LogCommandLinearizableRead, buffer.Bytes())
		assert.Nil(err)
		assert.Equal([]byte("ok"), result)
	})

	t.Run("read_linearize_error_shutdown", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		go func() {
			time.Sleep(100 * time.Millisecond)
			<-s.rpcAppendEntriesRequestChan
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.SubmitCommand(0, LogCommandLinearizableRead, buffer.Bytes())
		assert.ErrorIs(err, ErrShutdown)
	})

	t.Run("read_linearize_error_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.SubmitCommand(0, LogCommandLinearizableRead, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeout)
	})

	t.Run("command_not_found", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Follower

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: 99, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer))
		_, err := s.SubmitCommand(0, 100, buffer.Bytes())
		assert.ErrorIs(err, ErrLogCommandNotAllowed)
	})

	t.Run("write_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeout)
	})

	t.Run("write_response", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			time.Sleep(100 * time.Millisecond)
			data := <-s.rpcAppendEntriesRequestChan
			data.ResponseChan <- RPCResponse{
				Response: &raftypb.AppendEntryResponse{},
			}
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.Nil(err)
	})

	t.Run("write_error_shutdown", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		go func() {
			time.Sleep(100 * time.Millisecond)
			<-s.rpcAppendEntriesRequestChan
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrShutdown)
	})

	t.Run("write_error_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeout)
	})

	t.Run("write_forward_command_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Follower
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].Address, id: s.configuration.ServerMembers[0].ID})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.Error(err)
	})

	t.Run("read_no_leader_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		fsm := NewSnapshotState(s.logStore)
		s.fsm = fsm
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Follower

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadLeader(time.Second, LogCommandLinearizableRead, buffer.Bytes())
		assert.ErrorIs(err, ErrNoLeader)
	})

	t.Run("write_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeout)
	})
}

func TestClient_applyLogs(t *testing.T) {
	assert := assert.New(t)

	t.Run("entry_index_equal", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		logs := applyLogs{
			entries: []*raftypb.LogEntry{{}},
		}

		_, err := s.applyLogs(logs)
		assert.Nil(err)
	})

	t.Run("entry_wrong_log_type", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.lastApplied.Store(2)
		logs := applyLogs{
			entries: []*raftypb.LogEntry{
				{
					Index:   3,
					LogType: uint32(LogCommandReadLeaderLease),
				},
			},
		}

		_, err := s.applyLogs(logs)
		assert.Nil(err)
	})

	t.Run("error_apply_command", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.lastApplied.Store(2)
		logs := applyLogs{
			entries: []*raftypb.LogEntry{
				{
					Index:   3,
					LogType: uint32(LogReplication),
				},
			},
		}

		// the following fsm override is to simulate an error during apply
		fsm := NewSnapshotState(s.logStore)
		fsm.applyErrTest = fmt.Errorf("test induced error")
		s.fsm = fsm

		_, err := s.applyLogs(logs)
		assert.Error(err)
	})
}

func TestClient_bootstrapCluster(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		assert.ErrorIs(s.BootstrapCluster(time.Millisecond), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go s.commonLoop()
		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		assert.Nil(s.BootstrapCluster(0))
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		go func() {
			<-s.rpcBootstrapClusterRequestChan
			time.Sleep(2 * time.Second)
		}()
		assert.ErrorIs(s.BootstrapCluster(2*time.Second), ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcBootstrapClusterRequestChan
			time.Sleep(2 * time.Second)
		}()

		assert.ErrorIs(s.BootstrapCluster(time.Second), ErrTimeout)
		s.wg.Wait()
	})
}

func TestClient_addMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		assert.ErrorIs(s.AddMember(0, "127.0.0.1:6000", "60", false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		assert.ErrorIs(s.AddMember(0, "127.0.0.1:6000", "60", false), ErrShutdown)
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		assert.ErrorIs(s.AddMember(0, "127.0.0.1:6000", "60", false), ErrTimeout)
	})
}

func TestClient_promoteMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		s.configuration.ServerMembers[0].WaitToBePromoted = true
		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.PromoteMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		s.configuration.ServerMembers[0].WaitToBePromoted = true
		assert.ErrorIs(s.PromoteMember(0, node1.Address, node1.ID, false), ErrShutdown)
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		s.configuration.ServerMembers[0].WaitToBePromoted = true
		assert.ErrorIs(s.PromoteMember(0, node1.Address, node1.ID, false), ErrTimeout)
	})
}

func TestClient_demoteMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.DemoteMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.DemoteMember(0, node1.Address, node1.ID, false), ErrShutdown)
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.DemoteMember(0, node1.Address, node1.ID, false), ErrTimeout)
	})
}

func TestClient_removeMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.RemoveMember(0, node1.Address, node1.ID, false), ErrTimeout)
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.RemoveMember(0, node1.Address, node1.ID, false), ErrShutdown)
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.RemoveMember(0, node1.Address, node1.ID, false), ErrTimeout)
	})
}

func TestClient_forceRemoveMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.ForceRemoveMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		s.wg.Go(
			func() {
				time.Sleep(100 * time.Millisecond)
				data := <-s.rpcAppendEntriesRequestChan
				data.ResponseChan <- RPCResponse{
					Response: &raftypb.AppendEntryResponse{},
				}
			})

		node1 := s.configuration.ServerMembers[0]
		assert.Nil(s.ForceRemoveMember(0, node1.Address, node1.ID, false), nil)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		s.wg.Go(func() {
			time.Sleep(100 * time.Millisecond)
			<-s.rpcAppendEntriesRequestChan
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.ForceRemoveMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.ForceRemoveMember(0, node1.Address, node1.ID, false), ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_no_leader", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(5 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.ForceRemoveMember(0, node1.Address, node1.ID, false), ErrNoLeader)
	})

	t.Run("err_grpc_unavailable", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(6 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.Equal(status.Code(s.ForceRemoveMember(0, node1.Address, node1.ID, false)), codes.Unavailable)
	})
}

func TestClient_leaveOnTerminateMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			time.Sleep(time.Second)
			s.stopCtx()
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)

		s.wg.Go(
			func() {
				time.Sleep(100 * time.Millisecond)
				data := <-s.rpcAppendEntriesRequestChan
				data.ResponseChan <- RPCResponse{
					Response: &raftypb.AppendEntryResponse{},
				}
			})

		node1 := s.configuration.ServerMembers[0]
		assert.Nil(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false), nil)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		s.wg.Go(func() {
			time.Sleep(100 * time.Millisecond)
			<-s.rpcAppendEntriesRequestChan
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false), ErrTimeout)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcAppendEntriesRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false), ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_no_leader", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(5 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.ErrorIs(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false), ErrNoLeader)
	})

	t.Run("err_grpc_unavailable", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcAppendEntriesRequestChan
			time.Sleep(6 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		assert.Equal(status.Code(s.LeaveOnTerminateMember(0, node1.Address, node1.ID, false)), codes.Unavailable)
	})
}

func TestClient_status(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()
	s.State = Leader
	s.isRunning.Store(true)

	status := s.Status()
	assert.Equal(Leader, status.State)
}

func TestClient_IsBootstrapped(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()
	s.State = Leader
	s.options.BootstrapCluster = true
	s.isBootstrapped.Store(true)
	s.isRunning.Store(true)

	assert.Equal(s.IsBootstrapped(), true)
	s.State = Follower

	s.options.BootstrapCluster = false
	s.isBootstrapped.Store(false)
	assert.Equal(s.IsBootstrapped(), false)
}

func TestClient_IsLeader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()
	s.State = Leader
	s.isRunning.Store(true)

	assert.Equal(s.IsLeader(), true)
	s.State = Follower
	assert.Equal(s.IsLeader(), false)
}

func TestClient_Leader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()
	s.State = Leader
	s.isRunning.Store(true)

	found, address, id := s.Leader()
	assert.Equal(found, true)
	assert.Equal(address, s.Address.String())
	assert.Equal(id, s.id)

	s.State = Follower
	found, _, _ = s.Leader()
	assert.Equal(found, false)
}

func TestClient_FetchLeader(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.fillIDs()
	s.State = Follower
	s.isRunning.Store(true)
	found, _, _ := s.FetchLeader()
	assert.Equal(found, false)
}

func TestClient_isRunning(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	assert.Equal(false, s.IsRunning())
}

func TestClient_askForMembership(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	assert.Equal(false, s.AskForMembership())
}
