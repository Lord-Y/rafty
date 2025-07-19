package rafty

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient_FakeCommand(t *testing.T) {
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

	t.Run("no_leader", func(t *testing.T) {
		assert := assert.New(t)

		s := basicNodeSetup()
		err := s.parsePeers()
		assert.Nil(err)
		s.isRunning.Store(true)

		_, err = s.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s", s.id), Value: fmt.Sprintf("value%s", s.id)})
		assert.Error(err)
	})
}
