package rafty

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient_submitCommand(t *testing.T) {
	t.Run("fake_command", func(t *testing.T) {
		cc := clusterConfig{
			t:           t,
			testName:    "3_nodes_client_fake_command",
			clusterSize: 3,
			// runTestInParallel: true,
			portStartRange: 38000,
		}
		cc.assert = assert.New(t)

		time.AfterFunc(60*time.Second, func() {
			cc.submitFakeCommandOnAllNodes()
		})
		cc.testClustering(t)
	})

	t.Run("bootstrap", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.isRunning.Store(true)
		s.options.BootstrapCluster = true

		_, err = s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
	})

	t.Run("no_leader", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.isRunning.Store(true)
		s.isBootstrapped.Store(true)

		_, err = s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
	})

	t.Run("decode_command_error", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.isRunning.Store(true)
		s.State = Follower

		_, err = s.submitCommand([]byte("a=b"))
		assert.Error(err)
	})

	t.Run("command_timeout", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Follower
		s.setLeader(leaderMap{address: s.configuration.ServerMembers[0].address.String(), id: s.configuration.ServerMembers[0].ID})
		fmt.Println("xxxx", s.configuration.ServerMembers[0].address.String(), s.configuration.ServerMembers[0].ID)

		_, err = s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
	})

	t.Run("command_not_found", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.fillIDs()
		s.isRunning.Store(true)
		s.State = Follower

		_, err = s.SubmitCommand(Command{Kind: 99, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
	})
}
