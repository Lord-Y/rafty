package rafty

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStart3Nodes(t *testing.T) {
	cc := clusterConfig{
		t:                 t,
		testName:          "3_nodes",
		clusterSize:       3,
		runTestInParallel: false,
	}
	cc.testClustering(t)
}

func TestStart5Nodes(t *testing.T) {
	cc := clusterConfig{
		t:                         t,
		testName:                  "5_nodes",
		clusterSize:               5,
		delayLastNode:             true,
		delayLastNodeTimeDuration: time.Duration(60) * time.Second,
		autoSetMinimumClusterSize: true,
		portStartRange:            32000,
		runTestInParallel:         true,
	}
	cc.testClustering(t)
}

func TestStartNoDataDir(t *testing.T) {
	cc := clusterConfig{
		t:                 t,
		testName:          "3_nodes_noDataDir",
		clusterSize:       3,
		runTestInParallel: false,
		noDataDir:         true,
		portStartRange:    33000,
	}
	cc.testClustering(t)
}

func TestStartNoNodeID(t *testing.T) {
	cc := clusterConfig{
		t:                 t,
		testName:          "3_nodes_noNodeID",
		clusterSize:       3,
		runTestInParallel: false,
		noNodeID:          true,
		portStartRange:    34000,
	}
	cc.testClustering(t)
}

func TestStartTimeMultiplier(t *testing.T) {
	cc := clusterConfig{
		t:                 t,
		testName:          "3_nodes_timeMultiplier",
		clusterSize:       3,
		runTestInParallel: false,
		portStartRange:    35000,
		timeMultiplier:    11,
		maxAppendEntries:  1,
	}
	cc.testClustering(t)
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
