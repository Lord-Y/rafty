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
		s.isRunning.Store(true)
		s.options.BootstrapCluster = true

		_, err := s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("no_leader", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)

		_, err := s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("decode_command_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.isRunning.Store(true)
		s.State = Follower

		_, err := s.submitCommand([]byte("a=b"))
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("command_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Follower
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})

		_, err := s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("command_not_found", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Follower

		_, err := s.SubmitCommand(Command{Kind: 99, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("timeout_first", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		buffer := new(bytes.Buffer)
		err := EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer)
		assert.Nil(err)

		_, err = s.submitCommand(buffer.Bytes())
		assert.Error(err)
		assert.Nil(s.logStore.Close())
	})

	t.Run("timeout_second", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Leader
		s.setLeader(leaderMap{address: s.Address.String(), id: s.id})

		s.quitCtx, s.stopCtx = context.WithCancel(context.Background())
		defer s.stopCtx()

		go s.runAsLeader()

		go func() {
			time.Sleep(100 * time.Millisecond)
			s.stopCtx()
		}()

		buffer := new(bytes.Buffer)
		err := EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)}, buffer)
		assert.Nil(err)

		_, err = s.submitCommand(buffer.Bytes())
		assert.Error(err)
		assert.Nil(s.logStore.Close())
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

		assert.Error(s.BootstrapCluster(time.Millisecond))
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

		go s.commonLoop()
		assert.Error(s.BootstrapCluster(time.Second))
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

		assert.Error(s.BootstrapCluster(time.Second))
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
		assert.Error(err)
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
