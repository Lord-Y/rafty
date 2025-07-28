package rafty

import (
	"context"
	"fmt"
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
	"github.com/stretchr/testify/assert"
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
	peers := []Peer{
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
		Peers: peers,
	}
	s, _ := NewRafty(addr, id, options)

	for _, v := range s.options.Peers {
		s.configuration.ServerMembers = append(s.configuration.ServerMembers, peer{Address: v.Address})
	}
	return s
}

// singleServerClusterSetup is only a helper for other unit testing
func singleServerClusterSetup(address string) *Rafty {
	addr := net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(GRPCPort),
	}
	if address != "" {
		addr = getNetAddress(address)
	}

	id := fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := Options{
		IsSingleServerCluster: true,
	}
	s, _ := NewRafty(addr, id, options)
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
	noDataDir                 bool
	noNodeID                  bool
	maxAppendEntries          uint64
	readReplicaCount          uint64
	prevoteDisabled           bool
	isSingleServerCluster     bool
	bootstrapCluster          bool
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
			PersistDataOnDisk:     true,
		}
		server, err := NewRafty(addr, id, options)
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
		peers := []Peer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP("127.0.0.5"),
			Port: defaultPort + int(i),
		}

		options.logSource = cc.testName
		options.PrevoteDisabled = cc.prevoteDisabled
		options.TimeMultiplier = cc.timeMultiplier
		options.BootstrapCluster = cc.bootstrapCluster
		if cc.autoSetMinimumClusterSize {
			options.MinimumClusterSize = uint64(cc.clusterSize) - cc.readReplicaCount
		}
		options.MaxAppendEntries = cc.maxAppendEntries
		if cc.noDataDir && i != 0 || !cc.noDataDir {
			options.PersistDataOnDisk = true
			options.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, fmt.Sprintf("node%d", i))
		}
		if cc.readReplicaCount > 0 && int(cc.readReplicaCount) > readReplicaCount {
			options.ReadReplica = true
			readReplicaCount++
		}

		for j := range cc.clusterSize {
			var (
				peerAddr string
				err      error
			)

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
				peers = append(peers, Peer{
					Address: peerAddr,
					address: getNetAddress(peerAddr),
				})

				options.Peers = peers
				id := ""
				if !cc.noNodeID {
					id = fmt.Sprintf("%d", addr.Port)
					id = id[len(id)-2:]
				}
				server, err = NewRafty(addr, id, options)
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
		defaultPort = cc.cluster[0].options.Peers[cc.clusterSize-2].address.Port + 1
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
	if options.PersistDataOnDisk {
		options.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, "node"+id)
	}
	r, err := NewRafty(addr, id, options)
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := node.Start(); err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to start node")
					cc.assert.Errorf(err, "Fail to start cluster node %d with error", i)
					return
				}
			}()
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := node.Start(); err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to start node")
					cc.assert.Errorf(err, "Fail to start cluster node %d with error", i)
					return
				}
			}()
		})
	}
}

func (cc *clusterConfig) stopCluster(wg *sync.WaitGroup) {
	cc.t.Helper()
	for _, node := range cc.cluster {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		}()
	}

	for _, node := range cc.newNodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(time.Second)
			node = nil
		}()
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				node.Stop()
				// this sleep make sure all processings are done
				// and nothing remain before nillify the node
				time.Sleep(time.Second)
				node = nil
			}()
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
				node = nil
				node, err := NewRafty(address, id, options)
				cc.assert.Nil(err)
				cc.cluster[nodeId] = node
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

func (cc *clusterConfig) submitCommandOnAllNodes(wg *sync.WaitGroup, fake bool) {
	for i, node := range cc.cluster {
		cc.t.Run(fmt.Sprintf("%s_submitCommandToNode_%d", cc.testName, i), func(t *testing.T) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if fake {
					node.Logger.Info().Msgf("Submitting fake command to node %d", i)
					if _, err := node.SubmitCommand(Command{Kind: 99, Key: fmt.Sprintf("key%s%d", node.id, i), Value: fmt.Sprintf("value%d", i)}); err != nil {
						node.Logger.Error().Err(err).
							Str("node", fmt.Sprintf("%d", i)).
							Msgf("Failed to submit fake commmand to node")
						cc.assert.Error(err)
					}
					return
				}
				node.Logger.Info().Msgf("Submitting command to node %d", i)
				if _, err := node.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", node.id, i), Value: fmt.Sprintf("value%d", i)}); err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to submit commmand to node")
					cc.assert.Error(err)
				}
			}()
		})
	}
}

func (cc *clusterConfig) waitForLeader(timeout time.Duration, maxRound int) (leader leaderMap) {
	round := 0
	for {
		<-time.After(timeout)
		nodeId := rand.IntN(int(cc.clusterSize))
		node := cc.cluster[nodeId]

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
			cc.assert.Errorf(err, "Node %d reports fail to ask %s who is the leader", nodeId, node.Address.String())
			return
		}
		if response.LeaderAddress != "" && response.LeaderId != "" {
			leader.id, leader.address = response.LeaderId, response.LeaderAddress
			node.Logger.Info().Msgf("Node %d reports that %s / %s is the leader", nodeId, response.LeaderAddress, response.LeaderId)
			return
		}
		node.Logger.Info().Msgf("Node %d reports no leader found", nodeId)
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
	cc.submitCommandOnAllNodes(&wg, false)
	if cc.bootstrapCluster {
		cc.bootstrap(&wg)
		// provoke errors on purpuse on already bootstrapped cluster
		cc.bootstrap(&wg)
	}

	go func() {
		if leader := cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg, false)
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

		if leader := cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg, false)
			go cc.submitCommandOnAllNodes(&wg, true)
		}

	case <-timeoutGetLeader:
		go func() {
			if leader := cc.waitForLeader(time.Second, 3); leader != (leaderMap{}) {
				cc.submitCommandOnAllNodes(&wg, false)
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
	//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	wg.Wait()

	t.Logf("Test %s ran for %s", cc.testName, time.Since(now))
	t.Cleanup(func() {
		if shouldBeRemoved(dataDir) {
			_ = os.RemoveAll(dataDir)
		}
	})
}

func shouldBeRemoved(dir string) bool {
	return strings.Contains(filepath.Dir(dir), "rafty")
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
		if leader = cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
			go cc.submitCommandOnAllNodes(&wg, false)
			go cc.submitCommandOnAllNodes(&wg, true)
		}
	}()
	cc.startAdditionalNodes(&wg)
	cc.waitAdditionalNodesInCluster(&wg)

	time.Sleep(2 * time.Second)
	cc.stopAdditionalNode(&wg, "58")
	time.Sleep(2 * time.Second)

	response := &raftypb.MembershipChangeResponse{Success: true}
	cc.membershipAction(&wg, leader, "54", Demote, false, response)
	cc.membershipAction(&wg, leader, "55", Demote, false, response)
	time.Sleep(2 * time.Second)

	cc.membershipAction(&wg, leader, "54", Promote, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "54", Remove, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "55", ForceRemove, true, response)

	if leader = cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
		response.Success = true
		cc.membershipAction(&wg, leader, leader.id, ForceRemove, true, response)
	}

	time.Sleep(2 * time.Second)
	response.Success = true
	if leader = cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, leader, leader.id, Demote, false, response)
	}

	time.Sleep(2 * time.Second)
	response.Success = true
	if leader = cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, leader, "56", Demote, false, response)
	}

	time.Sleep(2 * time.Second)
	response.Success = true
	cc.membershipAction(&wg, leader, "56", Remove, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "57", Demote, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "57", Remove, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "53", Demote, false, response)
	time.Sleep(2 * time.Second)

	response.Success = true
	cc.membershipAction(&wg, leader, "53", Remove, false, response)

	response.Success = true
	cc.membershipAction(&wg, leader, leader.id, Demote, false, response)

	time.Sleep(2 * time.Second)
	response.Success = true
	if leader = cc.waitForLeader(2*time.Second, 5); leader != (leaderMap{}) {
		cc.membershipAction(&wg, leader, leader.id, Demote, false, response)
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

func (cc *clusterConfig) getMember(id string) (p peer, index int, originalCluster bool) {
	cc.t.Helper()
	if index := slices.IndexFunc(cc.cluster, func(p *Rafty) bool {
		return p.id == id
	}); index != -1 {
		return peer{Address: cc.cluster[index].Address.String(), ID: id, ReadReplica: cc.cluster[index].options.ReadReplica}, index, true
	}

	if index := slices.IndexFunc(cc.newNodes, func(p *Rafty) bool {
		return p.id == id
	}); index != -1 {
		return peer{Address: cc.newNodes[index].Address.String(), ID: id, ReadReplica: cc.newNodes[index].options.ReadReplica}, index, false
	}
	return
}

// waitAdditionalNodesInCluster will wait for additional nodes to be
// fully part of the cluster before doing anything else related to membership changes
func (cc *clusterConfig) waitAdditionalNodesInCluster(wg *sync.WaitGroup) {
	cc.t.Helper()
	wg.Add(1)
	defer wg.Done()

	count, round, maxRound := 0, 0, 10
	for {
		<-time.After(time.Second)
		for _, node := range cc.newNodes {
			if node.waitToBePromoted.Load() || node.askForMembership.Load() {
				count++
			}
			cc.t.Logf("MUST WAIT %s askForMembership %t waitToBePromoted %t count %d", node.id, node.askForMembership.Load(), node.waitToBePromoted.Load(), count)
		}
		if count == 0 {
			return
		}
		count = 0
		round++
		if round == maxRound {
			return
		}
	}
}

func (cc *clusterConfig) membershipAction(wg *sync.WaitGroup, leader leaderMap, id string, action MembershipChange, leaderShutdownOnRemove bool, expectedResponse *raftypb.MembershipChangeResponse) {
	cc.t.Run(fmt.Sprintf("%s_%s_%s", cc.testName, id, action.String()), func(t *testing.T) {
		wg.Add(1)
		defer wg.Done()

		member, index, originalCluster := cc.getMember(id)
		if member.Address != "" {
			if leader.id == id {
				if originalCluster {
					cc.cluster[index].options.ShutdownOnRemove = leaderShutdownOnRemove
				} else {
					cc.newNodes[index].options.ShutdownOnRemove = leaderShutdownOnRemove
				}
			}

			request := &raftypb.MembershipChangeRequest{
				Id:          member.ID,
				Address:     member.Address,
				ReadReplica: member.ReadReplica,
				Action:      uint32(action),
			}
			var noLeader bool
			for retry := range 3 {
				if noLeader {
					leader = cc.waitForLeader(time.Second, 3)
					noLeader = false
				}
				// we use retry here so r.rpcMembershipNotLeader from state_loop can be
				// covered by unit tests
				if leader.address == "" && retry > 0 {
					noLeader = true
					cc.t.Logf("Membership change request client no leader for %s retry %d", id+"_"+action.String(), retry)
				} else {
					cc.t.Logf("Membership change request client %s leaderAddress %s retry %d", id+"_"+action.String(), leader.address, retry)
					_, client, _ := cc.getClient(leader.address)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					response, err := client.SendMembershipChangeRequest(ctx, request)
					if err != nil {
						nerr := status.Convert(err)
						cc.t.Logf("Membership change request client error for %s leaderAddress %s retry %d error %s", id+"_"+action.String(), leader.address, retry, nerr.Err())
						if strings.Contains(nerr.Err().Error(), ErrNotLeader.Error()) {
							noLeader = true
							time.Sleep(time.Second)
						} else {
							if !slices.Contains([]error{ErrMembershipChangeNodeTooSlow, ErrMembershipChangeInProgress, ErrMembershipChangeNodeNotDemoted, ErrMembershipChangeNodeDemotionForbidden}, nerr.Err()) {
								cc.assert.Errorf(err, "Fail to send membership request to %s / %s, expected %t retry %d", member.Address, member.ID, expectedResponse.Success, retry)
							}
						}
					} else {
						cc.assert.Equal(expectedResponse.Success, response.Success, id+"_"+action.String())
						cc.t.Logf("Membership change request client success for %s result %t retry %d", id+"_"+action.String(), response.Success, retry)
						return
					}
				}
			}
		}
	})
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
