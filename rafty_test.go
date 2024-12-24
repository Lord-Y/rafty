package rafty

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	assert := assert.New(t)
	cc := clusterConfig{
		t:           t,
		clusterSize: 3,
	}
	cc.startCluster()
	time.Sleep(20 * time.Second)
	var found bool
	for !found {
		//nolint gosimple
		select {
		case <-time.After(30 * time.Millisecond):
			found, _, _ = cc.clientGetLeader()
			if found {
				for i, node := range cc.cluster {
					_, err := node.SubmitCommand(command{kind: commandSet, key: fmt.Sprintf("key%d", i), value: fmt.Sprintf("value%d", i)})
					assert.Nil(err)
				}
			}
		}
	}

	time.Sleep(2 * time.Second)
	node := 0
	err := cc.startOrStopSpecificicNode(node, "start")
	cc.cluster[node].Logger.Info().Msgf("error %s", err.Error())
	assert.Error(err)

	time.Sleep(5 * time.Second)
	err = cc.startOrStopSpecificicNode(node, "restart")
	assert.Nil(err)

	time.Sleep(90 * time.Second)
	cc.stopCluster()
}

func TestString(t *testing.T) {
	assert := assert.New(t)

	tests := []State{
		Down,
		ReadOnly,
		Follower,
		Candidate,
		Leader,
	}
	results := []string{
		"down",
		"readOnly",
		"follower",
		"candidate",
		"leader",
	}

	for k, v := range tests {
		assert.Equal(v.String() == results[k], true)
	}
}
