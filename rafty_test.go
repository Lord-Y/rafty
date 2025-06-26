package rafty

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStart3Nodes_normal(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes",
		clusterSize: 3,
		// runTestInParallel: true,
	}
	cc.assert = assert.New(t)
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
		// runTestInParallel:         true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestStart3Nodes_NoDataDir(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_noDataDir",
		clusterSize: 3,
		// runTestInParallel: true,
		noDataDir:      true,
		portStartRange: 33000,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestStart3Nodes_NoNodeID(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_noNodeID",
		clusterSize: 3,
		// runTestInParallel: true,
		noNodeID:       true,
		portStartRange: 34000,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestStart3Nodes_TimeMultiplier(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_timeMultiplier",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange:   35000,
		timeMultiplier:   11,
		maxAppendEntries: 1,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestStart7NodesWithReadOnly(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "7_nodes_with_read_only_nodes",
		clusterSize: 7,
		// runTestInParallel:         true,
		portStartRange:            36000,
		readOnlyNodeCount:         2,
		autoSetMinimumClusterSize: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestStart1Nodes_down_minimumSize(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "1Nodes_down_minimumSize",
		clusterSize: 3,
		// runTestInParallel:         true,
		portStartRange: 39000,
	}
	cc.assert = assert.New(t)
	cc.cluster = cc.makeCluster()
	id := 0
	node := cc.cluster[id]
	dataDir := filepath.Dir(node.options.DataDir)

	time.AfterFunc(20*time.Second, func() {
		node.Stop()
	})

	if err := node.Start(); err != nil {
		cc.t.Fatal("Fail to start node with error %w", err)
	}

	// double start to get error
	err := node.Start()
	cc.assert.NotNil(err)
	t.Cleanup(func() {
		if shouldBeRemoved(dataDir) {
			_ = os.RemoveAll(dataDir)
		}
		_ = os.Unsetenv("RAFTY_LOG_LEVEL")
	})
}

func TestStart3Nodes_PrevoteDisabled(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_prevote_disabled",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange: 40000,
		disablePrevote: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestNewRafty(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		id, IdExpected                                 string
		TimeMultiplier, TimeMultiplierExpected         uint
		MinimumClusterSize, MinimumClusterSizeExpected uint64
		MaxAppendEntries, MaxAppendEntriesExpected     uint64
		ElectionTimeout, ElectionTimeoutExpected       int
		HeartbeatTimeout, HeartbeatTimeoutExpected     int
		DataDir, DataDirExpected                       string
	}{
		{
			id:                         "",
			IdExpected:                 "",
			TimeMultiplier:             0,
			TimeMultiplierExpected:     1,
			MinimumClusterSize:         0,
			MinimumClusterSizeExpected: 3,
			MaxAppendEntries:           0,
			MaxAppendEntriesExpected:   1000,
			ElectionTimeout:            100,
			ElectionTimeoutExpected:    500,
			HeartbeatTimeout:           100,
			HeartbeatTimeoutExpected:   500,
			DataDir:                    "",
			DataDirExpected:            "not_empty",
		},
		{
			id:                         "x",
			IdExpected:                 "x",
			TimeMultiplier:             3,
			TimeMultiplierExpected:     3,
			MinimumClusterSize:         3,
			MinimumClusterSizeExpected: 3,
			MaxAppendEntries:           3000,
			MaxAppendEntriesExpected:   1000,
			ElectionTimeout:            600,
			ElectionTimeoutExpected:    600,
			HeartbeatTimeout:           600,
			HeartbeatTimeoutExpected:   600,
			DataDir:                    "/data",
			DataDirExpected:            "/data",
		},
		{
			id:                         "x",
			IdExpected:                 "x",
			TimeMultiplier:             15,
			TimeMultiplierExpected:     10,
			MinimumClusterSize:         3,
			MinimumClusterSizeExpected: 3,
			MaxAppendEntries:           3000,
			MaxAppendEntriesExpected:   1000,
			ElectionTimeout:            500,
			ElectionTimeoutExpected:    600,
			HeartbeatTimeout:           600,
			HeartbeatTimeoutExpected:   500,
			DataDir:                    "/data",
			DataDirExpected:            "/data",
		},
	}

	address := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.5"),
		Port: int(GRPCPort),
	}

	for _, tc := range tests {
		options := Options{
			TimeMultiplier:     tc.TimeMultiplier,
			MinimumClusterSize: tc.MinimumClusterSize,
			MaxAppendEntries:   tc.MaxAppendEntries,
			ElectionTimeout:    tc.ElectionTimeout,
			HeartbeatTimeout:   tc.HeartbeatTimeout,
			DataDir:            tc.DataDir,
		}

		r := NewRafty(address, tc.id, options)
		assert.Equal(tc.TimeMultiplierExpected, r.options.TimeMultiplier)
		assert.Equal(tc.MinimumClusterSizeExpected, r.options.MinimumClusterSize)
		assert.Equal(tc.MaxAppendEntriesExpected, r.options.MaxAppendEntries)
		assert.Equal(tc.ElectionTimeoutExpected, r.options.ElectionTimeout)
		assert.Equal(tc.HeartbeatTimeoutExpected, r.options.HeartbeatTimeout)
		if tc.DataDirExpected == "not_empty" {
			assert.NotEmpty(tc.DataDirExpected, r.options.DataDir)
		}
		assert.Equal(tc.IdExpected, r.id)
	}
}
