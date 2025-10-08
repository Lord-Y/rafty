package rafty

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/jackc/fake"
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// basicNodeSetup is only a helper for other unit testing.
// It MUST NOT be used to start a cluster
func basicNodeSetup() *Rafty {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	initialPeers := []InitialPeer{
		{
			Address: "127.0.0.1",
		},
		{
			Address: "127.0.0.2:50052",
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
	// set metrics namespace prefix to prevent panic cause by
	// duplicate metrics collector registration attempted
	options.MetricsNamespacePrefix = fmt.Sprintf("%s_%s", id, fake.CharactersN(100))
	storeOptions := BoltOptions{
		DataDir: options.DataDir,
		Options: bolt.DefaultOptions,
	}
	store, err := NewBoltStorage(storeOptions)
	if err != nil {
		log.Fatal(err)
	}
	fsm := NewSnapshotState(store)
	s, err := NewRafty(addr, id, options, store, store, fsm, nil)
	if err != nil {
		log.Fatal(err)
	}
	s.metrics = newMetrics(id, s.options.MetricsNamespacePrefix)
	s.metrics.setNodeStateGauge(Down)
	return s
}

type clusterConfig struct {
	t                         *testing.T
	assert                    *assert.Assertions
	runTestInParallel         bool
	testName                  string
	portStartRange            uint
	clusterSize               uint
	cluster                   []*Rafty
	newNodes                  []*Rafty
	timeMultiplier            uint
	delayLastNode             bool
	delayLastNodeTimeDuration time.Duration
	autoSetMinimumClusterSize bool
	noNodeID                  bool
	maxAppendEntries          uint64
	readReplicaCount          uint64
	prevoteDisabled           bool
	isSingleServerCluster     bool
	bootstrapCluster          bool
	mu                        sync.Mutex
	snapshotInterval          time.Duration
	snapshotThreshold         uint64
	electionTimeout           int
	heartbeatTimeout          int
}

func (cc *clusterConfig) makeCluster() (cluster []*Rafty) {
	defaultPort := int(GRPCPort)
	if cc.portStartRange == 0 {
		cc.portStartRange = 50000
	} else {
		defaultPort = int(cc.portStartRange) + 51
	}

	if cc.isSingleServerCluster {
		addr := net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: defaultPort,
		}

		id := fmt.Sprintf("%d", addr.Port)
		id = id[len(id)-2:]
		options := Options{
			IsSingleServerCluster: true,
			logSource:             cc.testName,
			DataDir:               filepath.Join(os.TempDir(), "rafty_test", cc.testName, id),
			ElectionTimeout:       cc.electionTimeout,
			HeartbeatTimeout:      cc.heartbeatTimeout,
		}
		// set metrics namespace prefix to prevent panic cause by
		// duplicate metrics collector registration attempted
		options.MetricsNamespacePrefix = fmt.Sprintf("%s_%s_%s", cc.testName, id, fake.CharactersN(100))
		storeOptions := BoltOptions{
			DataDir: options.DataDir,
			Options: bolt.DefaultOptions,
		}
		store, err := NewBoltStorage(storeOptions)
		cc.assert.Nil(err)
		fsm := NewSnapshotState(store)
		server, err := NewRafty(addr, id, options, store, store, fsm, nil)
		cc.assert.Nil(err)
		cluster = append(cluster, server)
		return
	}

	readReplicaCount := 0
	for i := range cc.clusterSize {
		var (
			addr    net.TCPAddr
			options Options
		)

		server := new(Rafty)
		initialPeers := []InitialPeer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP("127.0.0.5"),
			Port: defaultPort + int(i),
		}

		options.logSource = cc.testName
		options.PrevoteDisabled = cc.prevoteDisabled
		options.TimeMultiplier = cc.timeMultiplier
		options.BootstrapCluster = cc.bootstrapCluster
		options.SnapshotInterval = cc.snapshotInterval
		options.SnapshotThreshold = cc.snapshotThreshold
		options.ElectionTimeout = cc.electionTimeout
		options.HeartbeatTimeout = cc.heartbeatTimeout
		if cc.autoSetMinimumClusterSize {
			options.MinimumClusterSize = uint64(cc.clusterSize) - cc.readReplicaCount
		}
		options.MaxAppendEntries = cc.maxAppendEntries
		options.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, fmt.Sprintf("node%d", i))
		if cc.readReplicaCount > 0 && int(cc.readReplicaCount) > readReplicaCount {
			options.ReadReplica = true
			readReplicaCount++
		}
		storeOptions := BoltOptions{
			DataDir: options.DataDir,
			Options: bolt.DefaultOptions,
		}
		store, err := NewBoltStorage(storeOptions)
		cc.assert.Nil(err)

		for j := range cc.clusterSize {
			var peerAddr string

			if i == j {
				peerAddr = fmt.Sprintf("127.0.0.5:%d", cc.portStartRange+51+j)
			} else {
				if i > 0 {
					peerAddr = fmt.Sprintf("127.0.0.5:%d", cc.portStartRange+51+j)
				} else if i > 1 {
					peerAddr = fmt.Sprintf("127.0.0.5:%d", cc.portStartRange+51+j+i+2)
				} else {
					peerAddr = fmt.Sprintf("127.0.0.5:%d", cc.portStartRange+51+j+i)
				}
			}

			if addr.String() != peerAddr {
				initialPeers = append(initialPeers, InitialPeer{
					Address: peerAddr,
					address: getNetAddress(peerAddr),
				})

				options.InitialPeers = initialPeers
				id := ""
				if !cc.noNodeID {
					id = fmt.Sprintf("%d", addr.Port)
					id = id[len(id)-2:]
				}
				// set metrics namespace prefix to prevent panic cause by
				// duplicate metrics collector registration attempted
				options.MetricsNamespacePrefix = fmt.Sprintf("%s_%s_%s", cc.testName, id, fake.CharactersN(100))
				fsm := NewSnapshotState(store)
				server, err = NewRafty(addr, id, options, store, store, fsm, nil)
				cc.assert.Nil(err)
			}
		}
		cluster = append(cluster, server)
	}
	return
}

func (cc *clusterConfig) makeAdditionalNode(readReplica, shutdownOnRemove, leaveOnTerminate bool) {
	var defaultPort int
	if len(cc.newNodes) == 0 {
		defaultPort = cc.cluster[0].options.InitialPeers[cc.clusterSize-2].address.Port + 1
	} else {
		defaultPort = cc.newNodes[len(cc.newNodes)-1].Address.Port + 1
	}

	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.5"),
		Port: defaultPort,
	}
	id := fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := cc.cluster[0].options
	options.ReadReplica = readReplica
	options.ShutdownOnRemove = shutdownOnRemove
	options.LeaveOnTerminate = leaveOnTerminate
	options.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, "node"+id)
	storeOptions := BoltOptions{
		DataDir: options.DataDir,
		Options: bolt.DefaultOptions,
	}
	// set metrics namespace prefix to prevent panic cause by
	// duplicate metrics collector registration attempted
	options.MetricsNamespacePrefix = fmt.Sprintf("%s_%s_%s", cc.testName, id, fake.CharactersN(100))
	store, err := NewBoltStorage(storeOptions)
	cc.assert.Nil(err)
	fsm := NewSnapshotState(store)
	r, err := NewRafty(addr, id, options, store, store, fsm, nil)
	cc.assert.Nil(err)
	cc.newNodes = append(cc.newNodes, r)
}

func (cc *clusterConfig) startCluster(wg *sync.WaitGroup) {
	cc.cluster = cc.makeCluster()
	for i, node := range cc.cluster {
		if cc.delayLastNode && i == 0 {
			time.Sleep(cc.delayLastNodeTimeDuration)
		}
		cc.t.Run(fmt.Sprintf("cluster_%s_%d", cc.testName, i), func(t *testing.T) {
			wg.Go(func() {
				if err := node.Start(); err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to start node")
					cc.assert.Errorf(err, "Fail to start cluster node %d with error", i)
					return
				}
			})
		})
	}
}

func (cc *clusterConfig) startAdditionalNodes(wg *sync.WaitGroup) {
	cc.makeAdditionalNode(false, true, false)
	cc.makeAdditionalNode(true, true, false)
	cc.makeAdditionalNode(false, true, false)
	cc.makeAdditionalNode(false, false, false)
	cc.makeAdditionalNode(true, false, true)
	for i, node := range cc.newNodes {
		cc.t.Run(fmt.Sprintf("cluster_%s_newnode_%d", cc.testName, i), func(t *testing.T) {
			wg.Go(func() {
				if err := node.Start(); err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to start node")
					cc.assert.Errorf(err, "Fail to start cluster node %d with error", i)
					return
				}
			})
		})
	}
}

func (cc *clusterConfig) stopCluster(wg *sync.WaitGroup) {
	cc.t.Helper()
	for _, node := range cc.cluster {
		wg.Go(func() {
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		})
	}

	for _, node := range cc.newNodes {
		wg.Go(func() {
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		})
	}
}

func (cc *clusterConfig) forceStopCluster() {
	cc.t.Helper()
	for _, node := range cc.cluster {
		if node != nil {
			cc.t.Logf("Trying to force stop node %s / %s", node.Address.String(), node.id)
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		}
	}

	for _, node := range cc.newNodes {
		if node != nil {
			cc.t.Logf("Trying to force stop node %s / %s", node.Address.String(), node.id)
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		}
	}
}

func (cc *clusterConfig) stopAdditionalNode(wg *sync.WaitGroup, id string) {
	cc.t.Helper()
	for _, node := range cc.newNodes {
		if node.id == id {
			wg.Go(func() {
				node.Stop()
				// this sleep make sure all processings are done
				// and nothing remain before nillify the node
				time.Sleep(time.Second)
				node = nil
			})
		}
	}
}

func (cc *clusterConfig) restartNode(nodeId int, wg *sync.WaitGroup) {
	cc.t.Run(fmt.Sprintf("restart_%d", nodeId), func(t *testing.T) {
		wg.Add(1)
		defer wg.Done()
		node := cc.cluster[nodeId]
		node.Stop()

		var stop bool
		for !stop {
			<-time.After(time.Second)
			if !node.isRunning.Load() {
				stop = true

				id := node.id
				address := node.Address
				options := node.options
				cc.mu.Lock()
				node = nil
				storeOptions := BoltOptions{
					DataDir: options.DataDir,
					Options: bolt.DefaultOptions,
				}
				// set metrics namespace prefix to prevent panic cause by
				// duplicate metrics collector registration attempted
				options.MetricsNamespacePrefix = fmt.Sprintf("%s_%s_%s", cc.testName, id, fake.CharactersN(100))
				store, err := NewBoltStorage(storeOptions)
				cc.assert.Nil(err)
				fsm := NewSnapshotState(store)
				node, err := NewRafty(address, id, options, store, store, fsm, nil)
				cc.assert.Nil(err)
				cc.cluster[nodeId] = node
				cc.mu.Unlock()
				go func() {
					node.Logger.Info().Msgf("Restart node %s / %d", node.Address.String(), nodeId)
					if err := node.Start(); err != nil {
						node.Logger.Error().Err(err).
							Str("node", fmt.Sprintf("%d", nodeId)).
							Msgf("Failed to restart node")
						cc.assert.Errorf(err, "Fail to restart node %s / %d", nodeId, node.Address.String())
					}
				}()
			} else {
				node.Logger.Info().
					Str("node", fmt.Sprintf("%d", nodeId)).
					Msgf("Waiting for node to be completely stopped")
			}
		}
	})
}

func (cc *clusterConfig) submitCommandOnAllNodes(wg *sync.WaitGroup) {
	for i, node := range cc.cluster {
		cc.t.Run(fmt.Sprintf("%s_submitCommandToNode_%d", cc.testName, i), func(t *testing.T) {
			wg.Go(func() {
				node.Logger.Info().Msgf("Submitting command to node %d", i)
				bufferWrite := new(bytes.Buffer)
				key := fmt.Sprintf("key%s%d", node.id, i)
				if err := EncodeCommand(Command{Kind: CommandSet, Key: key, Value: fmt.Sprintf("value%d", i)}, bufferWrite); err != nil {
					node.Logger.Fatal().Err(err).Msgf("Fail to encode command %d", i)
				}
				if _, err := node.SubmitCommand(0, LogReplication, bufferWrite.Bytes()); err != nil {
					node.Logger.Error().Err(err).
						Str("node", node.id).
						Msgf("Failed to submit commmand to node")
					cc.assert.Error(err)
				}
				time.Sleep(100 * time.Millisecond)
				bufferRead := new(bytes.Buffer)
				if err := EncodeCommand(Command{Kind: CommandGet, Key: key}, bufferRead); err != nil {
					node.Logger.Fatal().Err(err).Msgf("Fail to encode command %d", i)
				}
				if _, err := node.SubmitCommand(0, LogCommandReadLeader, bufferRead.Bytes()); err != nil {
					node.Logger.Error().Err(err).
						Str("node", node.id).
						Str("logType", LogCommandReadLeader.String()).
						Msgf("Failed to submit commmand to node")
					cc.assert.Error(err)
				}
			})
		})
	}
}

func (cc *clusterConfig) waitForLeader(wg *sync.WaitGroup, timeout time.Duration, maxRound int) (leader leaderMap) {
	wg.Add(1)
	defer wg.Done()
	round := 0
	for {
		<-time.After(timeout)
		nodeId := rand.IntN(int(cc.clusterSize))
		cc.mu.Lock()
		node := cc.cluster[nodeId]
		cc.mu.Unlock()

		conn, client, err := cc.getClient(node.Address.String())
		if err != nil {
			cc.assert.Errorf(err, "Node %s / %s fail to connect to grpc server", node.Address.String(), node.id)
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.ClientGetLeader(ctx, &raftypb.ClientGetLeaderRequest{Message: "Who is the leader?"})
		if err != nil {
			cc.assert.Errorf(err, "Node %s / %s fail to ask %s who is the leader", node.Address.String(), node.id)
			return
		}
		if response.LeaderAddress != "" && response.LeaderId != "" {
			leader.id, leader.address = response.LeaderId, response.LeaderAddress
			node.Logger.Info().Msgf("Node %s / %s reports that %s / %s is the leader", node.Address.String(), node.id, response.LeaderAddress, response.LeaderId)
			return
		}
		node.Logger.Info().Msgf("Node %s / %s reports no leader found", node.Address.String(), node.id)
		round++
		if round >= maxRound {
			return leaderMap{}
		}
	}
}

func getUnitTestingTimeout() time.Duration {
	defaultTimeout := 3 * time.Minute
	timeout := os.Getenv("RAFTY_UNIT_TESTING_TIMEOUT")
	if timeout == "" {
		return defaultTimeout
	}
	z, err := time.ParseDuration(timeout)
	if err != nil {
		return defaultTimeout
	}
	return z
}

func (cc *clusterConfig) testClustering(t *testing.T) {
	t.Helper()
	if cc.runTestInParallel {
		t.Parallel()
	}
	now := time.Now()
	timeout := time.After(getUnitTestingTimeout())
	timeoutRestart := time.After(30 * time.Second)
	timeoutGetLeader := time.After(40 * time.Second)
	sleep := 50 * time.Second
	doneTest := make(chan bool)

	t.Setenv("RAFTY_LOG_LEVEL", "trace")
	var wg sync.WaitGroup
	cc.startCluster(&wg)
	dataDir := filepath.Dir(cc.cluster[0].options.DataDir)

	time.Sleep(5 * time.Second)
	// provoke errors on purpuse on non bootstrapped cluster
	cc.submitCommandOnAllNodes(&wg)
	if cc.bootstrapCluster {
		cc.bootstrap(&wg)
		// provoke errors on purpuse on already bootstrapped cluster
		cc.bootstrap(&wg)
	}

	go func() {
		if leader := cc.waitForLeader(&wg, 2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg)
		}
	}()

	select {
	case <-timeoutRestart:
		done, nodeId := false, 0
		for !done {
			go cc.restartNode(nodeId, &wg)
			if cc.isSingleServerCluster {
				done = true
			} else {
				if nodeId >= 2 {
					done = true
				}
				nodeId++
			}
			time.Sleep(2 * time.Second)
		}

		if leader := cc.waitForLeader(&wg, 2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg)
			go cc.submitCommandOnAllNodes(&wg)
		}

	case <-timeoutGetLeader:
		go func() {
			if leader := cc.waitForLeader(&wg, time.Second, 3); leader != (leaderMap{}) {
				cc.submitCommandOnAllNodes(&wg)
			}
		}()

	case <-timeout:
		cc.forceStopCluster()
		t.Fatal("Test didn't finish in time")
	case <-doneTest:
	}

	time.Sleep(sleep)
	cc.stopCluster(&wg)
	// pprof here is only to debug remaining go routines
	// _ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	wg.Wait()

	t.Logf("Test %s ran for %s", cc.testName, time.Since(now))
	t.Cleanup(func() {
		if shouldBeRemoved(dataDir) {
			_ = os.RemoveAll(dataDir)
		}
	})
}

func shouldBeRemoved(dir string) bool {
	return strings.Contains(filepath.Dir(dir), "rafty_test")
}

func getRootDir(dir string) string {
	pattern := "rafty_test"
	if strings.Contains(dir, pattern) {
		split := strings.Split(strings.TrimSuffix(dir, "/"), "/")
		if index := slices.Index(split, pattern); index != -1 {
			max := index + 2
			if len(split) > max {
				return strings.Join(split[0:max], "/")
			}
		}
	}
	return dir
}

func (cc *clusterConfig) testClusteringMembership(t *testing.T) {
	if cc.runTestInParallel {
		t.Parallel()
	}
	now := time.Now()

	t.Setenv("RAFTY_LOG_LEVEL", "trace")
	var wg sync.WaitGroup
	cc.startCluster(&wg)
	dataDir := filepath.Dir(cc.cluster[0].options.DataDir)

	var leader leaderMap
	go func() {
		if leader = cc.waitForLeader(&wg, 2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg)
		}
	}()
	cc.startAdditionalNodes(&wg)

	time.Sleep(5 * time.Second)
	cc.membershipAction(&wg, true, "54", Add, false)
	time.Sleep(5 * time.Second)

	cc.membershipAction(&wg, true, "55", Add, false)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, false, "56", Add, false)

	time.Sleep(2 * time.Second)
	cc.membershipAction(&wg, false, "57", Add, false)

	time.Sleep(2 * time.Second)
	cc.membershipAction(&wg, true, "58", Add, false)

	time.Sleep(5 * time.Second)
	cc.stopAdditionalNode(&wg, "58")
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, false, "54", Demote, false)
	time.Sleep(5 * time.Second)
	cc.membershipAction(&wg, true, "55", Demote, false)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, true, "54", Promote, false)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, true, "54", Remove, false)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, true, "55", ForceRemove, true)

	if leader = cc.waitForLeader(&wg, 5*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, true, leader.id, ForceRemove, true)
	}

	time.Sleep(2 * time.Second)
	if leader = cc.waitForLeader(&wg, 5*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, true, leader.id, Demote, false)
	}

	time.Sleep(2 * time.Second)
	if leader = cc.waitForLeader(&wg, 5*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, true, "56", Demote, false)
	}

	time.Sleep(2 * time.Second)
	cc.membershipAction(&wg, true, "56", Remove, false)
	time.Sleep(5 * time.Second)

	cc.membershipAction(&wg, true, "57", Demote, false)
	time.Sleep(5 * time.Second)

	cc.membershipAction(&wg, true, "57", Remove, false)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, true, "53", Demote, false)
	time.Sleep(5 * time.Second)

	cc.membershipAction(&wg, true, "53", Remove, false)

	if leader = cc.waitForLeader(&wg, 5*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, true, leader.id, Demote, false)
	}

	time.Sleep(5 * time.Second)
	if leader = cc.waitForLeader(&wg, 5*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, true, leader.id, Demote, false)
	}

	for _, node := range cc.cluster {
		t.Logf("Node %s / %s is running? %t", node.Address.String(), node.id, node.isRunning.Load())
	}
	for _, node := range cc.newNodes {
		t.Logf("Node %s / %s is running? %t", node.Address.String(), node.id, node.isRunning.Load())
	}

	cc.stopCluster(&wg)
	wg.Wait()

	t.Logf("Test %s ran for %s", cc.testName, time.Since(now))
	t.Cleanup(func() {
		if shouldBeRemoved(dataDir) {
			_ = os.RemoveAll(dataDir)
		}
	})
}

func (cc *clusterConfig) getMember(id string) (p Peer, index int, originalCluster bool) {
	cc.t.Helper()
	if index := slices.IndexFunc(cc.cluster, func(p *Rafty) bool {
		return p.id == id
	}); index != -1 {
		return Peer{Address: cc.cluster[index].Address.String(), ID: id, ReadReplica: cc.cluster[index].options.ReadReplica}, index, true
	}

	if index := slices.IndexFunc(cc.newNodes, func(p *Rafty) bool {
		return p.id == id
	}); index != -1 {
		return Peer{Address: cc.newNodes[index].Address.String(), ID: id, ReadReplica: cc.newNodes[index].options.ReadReplica}, index, false
	}
	return
}

func (cc *clusterConfig) membershipAction(wg *sync.WaitGroup, leader bool, id string, action MembershipChange, leaderShutdownOnRemove bool) {
	cc.t.Run(fmt.Sprintf("%s_%s_%s", cc.testName, id, action.String()), func(t *testing.T) {
		wg.Add(1)
		defer wg.Done()

		var node *Rafty
		if leader {
			leaderFound := cc.waitForLeader(wg, 5*time.Second, 5)
			if leaderFound != (leaderMap{}) {
				node = cc.getRaftNode(true)
			} else {
				node = cc.getRaftNode(true)
				if leaderShutdownOnRemove {
					node.options.ShutdownOnRemove = leaderShutdownOnRemove
				}
			}
		}

		for retry := range 3 {
			if node == nil {
				node = cc.getRaftNode(false)
			} else {
				cc.t.Logf("Membership change request client %s leaderAddress %s retry %d", id+"_"+action.String(), node.Address.String(), retry)
			}

			member, _, _ := cc.getMember(id)
			var membershipResponse error
			switch action {
			case Add:
				membershipResponse = node.AddMember(10*time.Second, member.Address, member.ID, member.ReadReplica)
			case Promote:
				membershipResponse = node.PromoteMember(10*time.Second, member.Address, member.ID, member.ReadReplica)
			case Demote:
				membershipResponse = node.DemoteMember(10*time.Second, member.Address, member.ID, member.ReadReplica)
			case Remove:
				membershipResponse = node.RemoveMember(10*time.Second, member.Address, member.ID, member.ReadReplica)
			case ForceRemove:
				membershipResponse = node.ForceRemoveMember(10*time.Second, member.Address, member.ID, member.ReadReplica)
			}

			if membershipResponse != nil {
				nerr := status.Convert(membershipResponse)
				cc.t.Logf("Membership change request client error for %s leaderAddress %s retry %d error %s", id+"_"+action.String(), node.Address.String(), retry, nerr.Err())
				if errors.Is(membershipResponse, ErrNotLeader) {
					// noLeader = true
					time.Sleep(time.Second)
				} else {
					if !slices.Contains([]error{ErrMembershipChangeNodeTooSlow, ErrMembershipChangeInProgress, ErrMembershipChangeNodeNotDemoted, ErrMembershipChangeNodeDemotionForbidden}, nerr.Err()) {
						cc.assert.Errorf(membershipResponse, "Fail to send membership request to %s / %s retry %d", member.Address, member.ID, retry)
					}
				}
			} else {
				cc.assert.Nil(membershipResponse, id+"_"+action.String())
				cc.t.Logf("Membership change request client is successfull for %s retry %d", id+"_"+action.String(), retry)
				return
			}
		}
	})
}

func (cc *clusterConfig) getRaftNode(leader bool) *Rafty {
	if leader {
		if index := slices.IndexFunc(cc.cluster, func(p *Rafty) bool {
			return p.getState() == Leader
		}); index != -1 {
			return cc.cluster[index]
		}
	}

	if index := slices.IndexFunc(cc.newNodes, func(p *Rafty) bool {
		return !p.options.ReadReplica && p.IsRunning()
	}); index != -1 {
		return cc.newNodes[index]
	}

	return nil
}

func (cc *clusterConfig) getClient(address string) (*grpc.ClientConn, raftypb.RaftyClient, error) {
	cc.t.Helper()
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, raftypb.NewRaftyClient(conn), nil
}

// fillIDs will fill peer ids
func (r *Rafty) fillIDs() {
	for i := range r.configuration.ServerMembers {
		id := fmt.Sprintf("%d", r.configuration.ServerMembers[i].address.Port)
		id = id[len(id)-2:]
		r.configuration.ServerMembers[i].ID = id
	}
}

func (cc *clusterConfig) bootstrap(wg *sync.WaitGroup) {
	node := cc.cluster[0]
	cc.t.Run(fmt.Sprintf("%s_bootstrap_node_%s", cc.testName, node.id), func(t *testing.T) {
		wg.Add(1)
		defer wg.Done()
		node.Logger.Info().Msgf("Submitting command to bootstrap node %s", node.id)
		conn, client, err := cc.getClient(node.Address.String())
		if err != nil {
			cc.assert.Errorf(err, "Node %s / %s fail to connect to grpc server", node.Address.String(), node.id)
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.SendBootstrapClusterRequest(ctx, &raftypb.BootstrapClusterRequest{})
		if err != nil {
			cc.t.Logf("Fail to bootstrap cluster node %s / %s with error %s", node.Address.String(), node.id, err)
			cc.assert.Errorf(err, "Fail to bootstrap cluster node %s / %s", node.Address.String(), node.id)
			return
		}
		if response.Success {
			node.Logger.Info().Msgf("Cluster bootstrapped on node %s / %s ", node.Address.String(), node.id)
			return
		}
	})
}
