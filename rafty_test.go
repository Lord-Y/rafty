package rafty

import (
	"fmt"
	"os"
	"strings"
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

	submitCommandToNode := func(nodeId int) {
		time.Sleep(20 * time.Second)
		var found bool
		for !found {
			//nolint gosimple
			select {
			case <-time.After(30 * time.Millisecond):
				found, _, _ = cc.clientGetLeader(nodeId)
				if found {
					for i, node := range cc.cluster {
						_, err := node.SubmitCommand(command{kind: commandSet, key: fmt.Sprintf("key%d%d", nodeId, i), value: fmt.Sprintf("value%d", i)})
						if err != nil {
							if strings.Contains(err.Error(), "the client connection is closing") {
								assert.Contains(err.Error(), "the client connection is closing")
							} else {
								assert.Equal(fmt.Errorf("NoLeader"), err)
							}
						} else {
							assert.Nil(err)
						}
					}
				}
			}
		}
	}

	os.Setenv("RAFTY_LOG_LEVEL", "trace")
	cc.startCluster()
	time.Sleep(2 * time.Second)

	startAndRestart := func(node int) {
		err := cc.startOrStopSpecificicNode(node, "start")
		cc.cluster[node].Logger.Info().Msgf("error %s", err.Error())
		assert.Error(err)

		time.Sleep(5 * time.Second)
		err = cc.startOrStopSpecificicNode(node, "restart")
		assert.Nil(err)

		time.Sleep(10 * time.Second)
		submitCommandToNode(node)
	}

	time.Sleep(30 * time.Second)
	node := 0
	go startAndRestart(node)

	time.Sleep(90 * time.Second)
	node = 1
	go startAndRestart(node)

	time.Sleep(90 * time.Second)
	node = 2
	go startAndRestart(node)

	time.Sleep(120 * time.Second)
	os.Unsetenv("RAFTY_LOG_LEVEL")
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
