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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer))
		_, err := s.SubmitCommand(0, LogReplication, buffer.Bytes())
		assert.ErrorIs(err, ErrNoLeader)
	})

	t.Run("read", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.SubmitCommand(0, LogCommandReadLeader, buffer.Bytes())
		assert.Error(err)

		_, err = s.SubmitCommand(0, LogCommandReadStale, buffer.Bytes())
		assert.Error(err)
	})

	t.Run("command_not_found", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
	})

	t.Run("write_response", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			time.Sleep(100 * time.Millisecond)
			data := <-s.triggerAppendEntriesChan
			data.responseChan <- appendEntriesResponse{Data: []byte("ok"), Error: nil}
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		go func() {
			time.Sleep(100 * time.Millisecond)
			<-s.triggerAppendEntriesChan
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		go func() {
			<-s.triggerAppendEntriesChan
			time.Sleep(2 * time.Second)
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandWrite(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
	})

	t.Run("write_forward_command_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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

	t.Run("read_leader_error_shutdown", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		go func() {
			time.Sleep(100 * time.Millisecond)
			<-s.triggerAppendEntriesChan
		}()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandGet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadLeader(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrShutdown)
	})

	t.Run("read_leader_error_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		// the following fsm override is to simulate an error during apply
		fsm := NewSnapshotState(s.logStore)
		fsm.sleepErr = 2 * time.Second
		s.fsm = fsm
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadLeader(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
	})

	t.Run("read_no_leader_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		// the following fsm override is to simulate an error during apply
		fsm := NewSnapshotState(s.logStore)
		fsm.sleepErr = 2 * time.Second
		s.fsm = fsm
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Follower

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadLeader(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrNoLeader)
	})

	t.Run("read_stale_shutdown_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		// the following fsm override is to simulate an error during apply
		fsm := NewSnapshotState(s.logStore)
		fsm.sleepErr = 2 * time.Second
		s.fsm = fsm
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadStale(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrShutdown)
	})

	t.Run("read_stale_error_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		// the following fsm override is to simulate an error during apply
		fsm := NewSnapshotState(s.logStore)
		fsm.sleepErr = 2 * time.Second
		s.fsm = fsm
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		assert.Nil(EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id)}, buffer))

		_, err := s.submitCommandReadStale(time.Second, buffer.Bytes())
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
	})
}

func TestClient_applyLogs(t *testing.T) {
	assert := assert.New(t)

	t.Run("entry_index_equal", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Follower
		s.lastApplied.Store(2)
		logs := applyLogs{
			entries: []*raftypb.LogEntry{
				{
					Index:   3,
					LogType: uint32(LogCommandReadLeader),
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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

		assert.ErrorIs(s.BootstrapCluster(time.Millisecond), ErrTimeoutSendingRequest)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
			assert.Nil(os.RemoveAll(s.options.DataDir))
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

		assert.ErrorIs(s.BootstrapCluster(time.Second), ErrTimeoutSendingRequest)
		s.wg.Wait()
	})
}

func TestClient_demoteMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
		_, err := s.DemoteMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		forgedResponse := RPCResponse{
			Response: &raftypb.MembershipChangeResponse{Success: true},
		}
		s.wg.Go(func() {
			for {
				select {
				case data := <-s.rpcMembershipChangeRequestChan:
					data.ResponseChan <- forgedResponse
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		response, err := s.DemoteMember(0, node1.Address, node1.ID, false)
		assert.NotNil(response)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcMembershipChangeRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		_, err := s.DemoteMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcMembershipChangeRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		_, err := s.DemoteMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})
}

func TestClient_removeMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
		_, err := s.RemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		forgedResponse := RPCResponse{
			Response: &raftypb.MembershipChangeResponse{Success: true},
		}
		s.wg.Go(func() {
			for {
				select {
				case data := <-s.rpcMembershipChangeRequestChan:
					data.ResponseChan <- forgedResponse
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		response, err := s.RemoveMember(0, node1.Address, node1.ID, false)
		assert.NotNil(response)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcMembershipChangeRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		_, err := s.RemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcMembershipChangeRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		_, err := s.RemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})
}

func TestClient_forceRemoveMember(t *testing.T) {
	assert := assert.New(t)

	t.Run("timeout_first", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
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
		_, err := s.ForceRemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})

	t.Run("err_nil", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		forgedResponse := RPCResponse{
			Response: &raftypb.MembershipChangeResponse{Success: true},
		}
		s.wg.Go(func() {
			for {
				select {
				case data := <-s.rpcMembershipChangeRequestChan:
					data.ResponseChan <- forgedResponse
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		response, err := s.ForceRemoveMember(0, node1.Address, node1.ID, false)
		assert.NotNil(response)
		assert.Nil(err)
		s.wg.Wait()
	})

	t.Run("quit_context_done", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Leader
		s.isRunning.Store(true)
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		s.stopCtx()

		s.wg.Go(func() {
			for {
				select {
				case <-s.rpcMembershipChangeRequestChan:
				case <-time.After(500 * time.Millisecond):
					return
				}
			}
		})

		node1 := s.configuration.ServerMembers[0]
		_, err := s.ForceRemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrShutdown)
		s.wg.Wait()
	})

	t.Run("err_timeout", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(s.options.DataDir))
		}()
		s.fillIDs()
		s.State = Follower
		s.options.BootstrapCluster = true
		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go func() {
			<-s.rpcMembershipChangeRequestChan
			time.Sleep(2 * time.Second)
		}()

		node1 := s.configuration.ServerMembers[0]
		_, err := s.ForceRemoveMember(0, node1.Address, node1.ID, false)
		assert.ErrorIs(err, ErrTimeoutSendingRequest)
		s.wg.Wait()
	})
}

func TestClient_status(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(s.options.DataDir))
	}()
	s.fillIDs()
	s.State = Leader
	s.isRunning.Store(true)

	status := s.Status()
	assert.Equal(Leader, status.State)
}
