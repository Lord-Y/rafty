package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient_FakeCommand(t *testing.T) {
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
}
