package rafty

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
)

func TestRafty_newRafty(t *testing.T) {
	assert := assert.New(t)

	t.Run("options", func(t *testing.T) {
		fakeDataDir := filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "newrafty")
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
				DataDir:                    fakeDataDir,
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
				DataDir:                    fakeDataDir,
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

			storeOptions := BoltOptions{
				DataDir: options.DataDir,
				Options: bolt.DefaultOptions,
			}
			if tc.DataDir == "" {
				store, err := NewBoltStorage(storeOptions)
				assert.ErrorIs(err, ErrDataDirRequired)
				fsm := NewSnapshotState(store)
				_, err = NewRafty(address, tc.id, options, store, store, fsm, nil)
				assert.ErrorIs(err, ErrDataDirRequired)
			} else {
				store, err := NewBoltStorage(storeOptions)
				assert.Nil(err)
				fsm := NewSnapshotState(store)
				snapshot := NewSnapshot(options.DataDir, 3)
				r, err := NewRafty(address, tc.id, options, store, store, fsm, snapshot)
				assert.Nil(err)
				assert.Equal(tc.TimeMultiplierExpected, r.options.TimeMultiplier)
				assert.Equal(tc.MinimumClusterSizeExpected, r.options.MinimumClusterSize)
				assert.Equal(tc.MaxAppendEntriesExpected, r.options.MaxAppendEntries)
				assert.Equal(tc.ElectionTimeoutExpected, r.options.ElectionTimeout)
				assert.Equal(tc.HeartbeatTimeoutExpected, r.options.HeartbeatTimeout)
				assert.Equal(tc.IdExpected, r.id)
				assert.Nil(store.Close())
			}
		}
	})

	t.Run("metadata_panic", func(t *testing.T) {
		addr := net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: int(GRPCPort),
		}
		initialPeers := []InitialPeer{
			{
				Address: "127.0.0.1",
			},
			{
				Address: "127.0.0.2",
			},
			{
				Address: "127.0.0.3:50053",
			},
		}

		id := fmt.Sprintf("%d", addr.Port)
		id = id[len(id)-2:]
		options := Options{
			InitialPeers: initialPeers,
			DataDir:      filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "basic_setup", id),
		}
		storeOptions := BoltOptions{
			DataDir: options.DataDir,
			Options: bolt.DefaultOptions,
		}
		defer func() {
			_ = os.RemoveAll(options.DataDir)
		}()
		store, err := NewBoltStorage(storeOptions)
		if err != nil {
			log.Fatal(err)
		}
		assert.Nil(store.StoreMetadata([]byte("a=b")))
		assert.Nil(store.Close())
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		fsm := NewSnapshotState(store)
		_, _ = NewRafty(addr, id, options, store, store, fsm, nil)
	})

	t.Run("start_panic", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.id = ""
		assert.Nil(s.logStore.Close())
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered. Error:\n", r)
			}
		}()
		assert.Error(s.Start())
	})
}

func TestRafty_restore(t *testing.T) {
	assert := assert.New(t)

	t.Run("metadata_success", func(t *testing.T) {
		s := basicNodeSetup()
		s.fillIDs()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.currentTerm.Store(1)
		peers, _ := s.getPeers()
		newbie := Peer{Address: "127.0.0.1:60000", ID: "xyz"}
		peers = append(peers, newbie)
		encodedPeers := EncodePeers(peers)
		assert.NotNil(encodedPeers)
		s.options.BootstrapCluster = true

		for index := range 100 {
			entry := makeNewLogEntry(1, LogReplication, []byte(fmt.Sprintf("%s=%d", fake.WordsN(5), index)))
			logs := []*LogEntry{entry}
			s.storeLogs(logs)
			assert.Nil(s.applyConfigEntry(makeProtobufLogEntry(entry)[0]))
		}
		_, err := s.takeSnapshot()
		assert.Nil(err)

		assert.Nil(s.clusterStore.StoreMetadata(s.buildMetadata()))
		metadata, err := s.clusterStore.GetMetadata()
		assert.Nil(err)
		s.isBootstrapped.Store(false)
		assert.Nil(s.restore(metadata))
	})

	t.Run("metadata_error", func(t *testing.T) {
		s := basicNodeSetup()
		s.fillIDs()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		assert.Nil(s.clusterStore.StoreMetadata([]byte("a=b")))
		metadata, err := s.clusterStore.GetMetadata()
		assert.Nil(err)
		s.isBootstrapped.Store(false)
		assert.Error(s.restore(metadata))
	})

	t.Run("metadata_newrafty_restore_error", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.fillIDs()
		s.isBootstrapped.Store(false)
		assert.Nil(s.clusterStore.StoreMetadata([]byte("a=b")))
		assert.Nil(s.logStore.Close())
		options := s.options

		storeOptions := BoltOptions{
			DataDir: options.DataDir,
			Options: bolt.DefaultOptions,
		}
		store, err := NewBoltStorage(storeOptions)
		assert.Nil(err)
		fsm := NewSnapshotState(store)
		_, err = NewRafty(s.Address, s.id, options, store, store, fsm, nil)
		assert.Error(err)
		assert.Nil(store.Close())
	})
}

func TestRafty_checkNodeIDs(t *testing.T) {
	assert := assert.New(t)
	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()

	t.Run("empty", func(t *testing.T) {
		s.checkNodeIDs()
	})

	t.Run("filled", func(t *testing.T) {
		s.fillIDs()
		s.checkNodeIDs()
	})
}

func TestRafty_built_metadata(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(s.logStore.Close())
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.currentTerm.Store(1)
	s.lastApplied.Store(1)

	assert.NotNil(s.buildMetadata())
}

func TestRafty_storeLogs(t *testing.T) {
	assert := assert.New(t)

	s := basicNodeSetup()
	defer func() {
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()
	s.currentTerm.Store(1)
	s.lastApplied.Store(1)
	s.State = Leader

	assert.Nil(s.logStore.Close())
	entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
	logs := []*LogEntry{entry}
	assert.Error(s.storeLogs(logs))
	assert.Equal(s.State, Follower)
}

func TestRafty_getPreviousLogIndexAndTerm(t *testing.T) {
	assert := assert.New(t)

	t.Run("zero", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		index, term := s.getPreviousLogIndexAndTerm()
		assert.Equal(uint64(0), index)
		assert.Equal(uint64(0), term)
	})

	t.Run("next_index", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.nextIndex.Store(2)
		s.lastLogIndex.Store(2)
		s.lastIncludedIndex.Store(1)
		s.lastIncludedTerm.Store(1)

		index, term := s.getPreviousLogIndexAndTerm()
		assert.Equal(s.lastIncludedIndex.Load(), index)
		assert.Equal(s.lastIncludedTerm.Load(), term)
	})

	t.Run("error_zero", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.nextIndex.Store(2)
		s.lastLogIndex.Store(50)
		s.lastLogTerm.Store(1)

		index, term := s.getPreviousLogIndexAndTerm()
		assert.Equal(uint64(0), index)
		assert.Equal(uint64(0), term)
	})

	t.Run("success", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()

		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		s.nextIndex.Store(2)

		index, term := s.getPreviousLogIndexAndTerm()
		assert.Equal(s.lastLogIndex.Load(), index)
		assert.Equal(s.currentTerm.Load(), term)
	})
}

func TestRafty_getPreviousLogIndexAndTermWithError(t *testing.T) {
	assert := assert.New(t)

	t.Run("error_is_returned", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.nextIndex.Store(2)
		s.lastLogIndex.Store(50)
		s.lastLogTerm.Store(1)

		index, term, err := s.getPreviousLogIndexAndTermWithError()
		assert.Equal(uint64(0), index)
		assert.Equal(uint64(0), term)
		assert.Error(err)
	})

	t.Run("success", func(t *testing.T) {
		s := basicNodeSetup()
		defer func() {
			assert.Nil(s.logStore.Close())
			assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
		}()
		s.currentTerm.Store(1)
		entry := makeNewLogEntry(s.currentTerm.Load(), LogReplication, []byte("a=b"))
		logs := []*LogEntry{entry}
		assert.Nil(s.storeLogs(logs))
		s.nextIndex.Store(2)

		index, term, err := s.getPreviousLogIndexAndTermWithError()
		assert.Nil(err)
		assert.Equal(s.lastLogIndex.Load(), index)
		assert.Equal(s.currentTerm.Load(), term)
	})
}

func TestRafty_serve_withContextCancel(t *testing.T) {
	assert := assert.New(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skipping serve test: cannot allocate local listener: %v", err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	assert.Nil(listener.Close())

	options := Options{
		IsVoter:      true,
		InitialPeers: []InitialPeer{{Address: "127.0.0.2:50052"}, {Address: "127.0.0.3:50053"}},
		DataDir:      filepath.Join(os.TempDir(), "rafty_test", fake.CharactersN(20), "serve_cancel"),
	}
	options.MetricsNamespacePrefix = fmt.Sprintf("serve_%d_%s", addr.Port, fake.CharactersN(20))
	storeOptions := BoltOptions{
		DataDir: options.DataDir,
		Options: bolt.DefaultOptions,
	}
	store, err := NewBoltStorage(storeOptions)
	assert.Nil(err)
	fsm := NewSnapshotState(store)
	s, err := NewRafty(*addr, fmt.Sprintf("%d", addr.Port), options, store, store, fsm, nil)
	assert.Nil(err)
	defer func() {
		assert.Nil(os.RemoveAll(getRootDir(s.options.DataDir)))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Serve(ctx)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		assert.Nil(err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}

func TestRafty_start3Nodes_normal(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes",
		clusterSize: 3,
		// runTestInParallel: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start5Nodes_normal(t *testing.T) {
	cc := clusterConfig{
		t:                         t,
		testName:                  "5_nodes_normal",
		clusterSize:               5,
		delayLastNode:             true,
		delayLastNodeTimeDuration: 5 * time.Second,
		autoSetMinimumClusterSize: true,
		portStartRange:            32000,
		// runTestInParallel:         true,
		snapshotInterval:  5 * time.Second,
		snapshotThreshold: 2,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start3Nodes_NoNodeID(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_noNodeID",
		clusterSize: 3,
		// runTestInParallel: true,
		noNodeID:       true,
		portStartRange: 33000,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start3Nodes_TimeMultiplier(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_timeMultiplier",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange:   34000,
		timeMultiplier:   11,
		maxAppendEntries: 1,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start7NodesWithNonVoters(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "7_nodes_with_read_only_nodes",
		clusterSize: 7,
		// runTestInParallel:         true,
		portStartRange:            35000,
		nonVoterCount:             2,
		autoSetMinimumClusterSize: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start1Nodes_down_minimumSize(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "1Nodes_down_minimumSize",
		clusterSize: 3,
		// runTestInParallel:         true,
		portStartRange: 37000,
	}
	cc.assert = assert.New(t)
	cc.cluster = cc.makeCluster()
	node1 := cc.cluster[0]
	node2 := cc.cluster[1]
	node2.Address = node1.Address
	dataDir1 := filepath.Dir(node1.options.DataDir)
	dataDir2 := filepath.Dir(node2.options.DataDir)

	time.AfterFunc(8*time.Second, func() {
		node1.Stop()
		node2.Stop()
	})

	go func() {
		time.Sleep(100 * time.Millisecond)
		cc.assert.NotNil(node2.Start())
	}()

	if err := node1.Start(); err != nil {
		cc.t.Fatal("Fail to start node with error %w", err)
	}

	t.Cleanup(func() {
		if shouldBeRemoved(dataDir1) {
			_ = os.RemoveAll(dataDir1)
		}
		if shouldBeRemoved(dataDir2) {
			_ = os.RemoveAll(dataDir2)
		}
	})
}

func TestRafty_start3Nodes_PrevoteDisabled(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_prevote_disabled",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange:  38000,
		prevoteDisabled: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start3Nodes_membership(t *testing.T) {
	cc := clusterConfig{
		t:                t,
		testName:         "3_nodes_membership",
		clusterSize:      3,
		electionTimeout:  2000,
		heartbeatTimeout: 1000,
		// runTestInParallel: true,
		portStartRange: 39000,
	}
	cc.assert = assert.New(t)
	cc.testClusteringMembership(t)
}

func TestRafty_startSingleServerCluster(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "single_server_cluster",
		clusterSize: 1,
		// runTestInParallel: true,
		portStartRange:        40000,
		isSingleServerCluster: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start3Nodes_bootstrap_cluster(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_bootstrap_cluster",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange:   41000,
		bootstrapCluster: true,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}

func TestRafty_start3Nodes_snapshot(t *testing.T) {
	cc := clusterConfig{
		t:           t,
		testName:    "3_nodes_snapshot",
		clusterSize: 3,
		// runTestInParallel: true,
		portStartRange:    42000,
		bootstrapCluster:  true,
		snapshotInterval:  5 * time.Second,
		snapshotThreshold: 2,
	}
	cc.assert = assert.New(t)
	cc.testClustering(t)
}
