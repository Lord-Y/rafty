package rafty

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterConfig struct {
	t                         *testing.T
	assert                    *assert.Assertions
	runTestInParallel         bool
	testName                  string
	portStartRange            uint
	clusterSize               uint
	cluster                   []*Rafty
	timeMultiplier            uint
	delayLastNode             bool
	delayLastNodeTimeDuration time.Duration
	autoSetMinimumClusterSize bool
	noDataDir                 bool
	noNodeID                  bool
	maxAppendEntries          uint64
	readOnlyNodeCount         uint64
	disablePrevote            bool
}

func (cc *clusterConfig) makeCluster() (cluster []*Rafty) {
	defaultPort := int(GRPCPort)
	if cc.portStartRange == 0 {
		cc.portStartRange = 50000
	} else {
		defaultPort = int(cc.portStartRange) + 51
	}
	readOnlyNodeCount := 0
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

		options.DisablePrevote = cc.disablePrevote
		options.TimeMultiplier = cc.timeMultiplier
		if cc.autoSetMinimumClusterSize {
			options.MinimumClusterSize = uint64(cc.clusterSize) - cc.readOnlyNodeCount
		}
		options.MaxAppendEntries = cc.maxAppendEntries
		if cc.noDataDir && i != 0 || !cc.noDataDir {
			options.PersistDataOnDisk = true
			options.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, fmt.Sprintf("node%d", i))
		}
		if cc.readOnlyNodeCount > 0 && int(cc.readOnlyNodeCount) > readOnlyNodeCount {
			options.ReadOnlyNode = true
			readOnlyNodeCount++
		}

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
				peers = append(peers, Peer{
					Address: peerAddr,
				})

				options.Peers = peers
				options.logSource = cc.testName
				id := ""
				if !cc.noNodeID {
					id = fmt.Sprintf("%d", i)
				}
				server = NewRafty(addr, id, options)
			}
		}
		cluster = append(cluster, server)
	}
	return
}

func (cc *clusterConfig) startCluster() {
	cc.cluster = cc.makeCluster()
	for i, node := range cc.cluster {
		if cc.delayLastNode && i == 0 {
			time.Sleep(cc.delayLastNodeTimeDuration)
		}
		cc.t.Run(fmt.Sprintf("cluster_%s_%d", cc.testName, i), func(t *testing.T) {
			time.AfterFunc(time.Second, func() {
				go func() {
					if err := node.Start(); err != nil {
						node.Logger.Error().Err(err).
							Str("node", fmt.Sprintf("%d", i)).
							Msgf("Failed to start node")
						cc.assert.Errorf(err, "Fail to start cluster node %d with error", i)
						return
					}
				}()
			})
		})
	}
}

func (cc *clusterConfig) stopCluster(wg *sync.WaitGroup) {
	for _, node := range cc.cluster {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.Stop()
			// this sleep make sure all processings are done
			// and nothing remain before nillify the node
			time.Sleep(10 * time.Second)
			node = nil
		}()
	}
}

func (cc *clusterConfig) restartNode(nodeId int, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	cc.t.Run(fmt.Sprintf("restart_%d", nodeId), func(t *testing.T) {
		node := cc.cluster[nodeId]
		node.Stop()

		var stop bool
		for !stop {
			<-time.After(time.Second)
			if !node.isRunning.Load() {
				stop = true
				// reset this part to prevent errors
				metaFile, dataFile := node.newStorage()
				node.storage = storage{
					metadata: metaFile,
					data:     dataFile,
				}
				node.storage.metadata.rafty = node
				node.storage.data.rafty = node
				node.logs = node.newLogs()
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

func (cc *clusterConfig) submitCommandOnAllNodes(wg *sync.WaitGroup) (count atomic.Uint64) {
	// it will look weird to have 2 synchronizations but the first defer
	// make sure that all subtests will finish properly before nillify cc variable
	defer wg.Done()
	var wgi sync.WaitGroup
	for i, node := range cc.cluster {
		wgi.Add(1)
		go func() {
			defer wgi.Done()
			cc.t.Run(fmt.Sprintf("%s_submitCommandToNode_%d", cc.testName, i), func(t *testing.T) {
				node.Logger.Info().Msgf("Submitting command to node %d", i)
				_, err := node.SubmitCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key%s%d", node.id, i), Value: fmt.Sprintf("value%d", i)})
				if err != nil {
					node.Logger.Error().Err(err).
						Str("node", fmt.Sprintf("%d", i)).
						Msgf("Failed to submit commmand to node")
					cc.assert.Error(err)
				} else {
					count.Add(1)
					cc.assert.Nil(err)
				}
			})
		}()
	}
	wgi.Wait()
	return
}

// func (cc *clusterConfig) countLogsOnAllNodes(count uint64) {
// 	var wg sync.WaitGroup
// 	for i, node := range cc.cluster {
// 		wg.Add(1)
// 		cc.t.Run(fmt.Sprintf("%s_countLogsOnNode_%d", cc.testName, i), func(t *testing.T) {
// 			defer wg.Done()
// 			node.mu.Lock()
// 			totalLogs := len(node.logs.log)
// 			node.mu.Unlock()
// 			cc.assert.GreaterOrEqual(uint64(totalLogs), count)
// 			cc.assert.Greater(totalLogs, 1)
// 		})
// 	}
// 	wg.Wait()
// }

func (cc *clusterConfig) submitFakeCommandOnAllNodes() {
	i := 0
	node := cc.cluster[i]
	cc.t.Run(fmt.Sprintf("%s_submitCommandToNode_%d", cc.testName, i), func(t *testing.T) {
		_, err := node.SubmitCommand(Command{Kind: 99, Key: fmt.Sprintf("key%s%d", node.id, i), Value: fmt.Sprintf("value%d", i)})
		if err != nil {
			node.Logger.Error().Err(err).
				Str("node", fmt.Sprintf("%d", i)).
				Msgf("Failed to submit commmand to node")
			cc.assert.Error(err)
			return
		}
		cc.assert.Nil(err)
	})
}

func (cc *clusterConfig) waitForLeader() (leader leaderMap) {
	round := 0
	for {
		<-time.After(3 * time.Second)
		nodeId := rand.IntN(int(cc.clusterSize))
		node := cc.cluster[nodeId]

		conn, err := grpc.NewClient(
			node.Address.String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			cc.assert.Errorf(err, "Node %d reports fail to connect to grpc server %s", nodeId, node.Address.String())
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		client := raftypb.NewRaftyClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.ClientGetLeader(ctx, &raftypb.ClientGetLeaderRequest{Message: "Who is the leader?"})
		if err != nil {
			cc.assert.Errorf(err, "Node %d reports fail to ask %s who is the leader", nodeId, node.Address.String())
			return
		}
		if response.LeaderAddress != "" && response.LeaderID != "" {
			leader.id, leader.address = response.LeaderID, response.LeaderAddress
			node.Logger.Info().Msgf("Node %d reports that %s / %s is the leader", nodeId, response.LeaderAddress, response.LeaderID)
			return
		}
		node.Logger.Info().Msgf("Node %d reports no leader found", nodeId)
		round++
		if round >= 5 {
			return leaderMap{}
		}
	}
}

func (cc *clusterConfig) testClustering(t *testing.T) {
	if cc.runTestInParallel {
		t.Parallel()
	}

	_ = os.Setenv("RAFTY_LOG_LEVEL", "trace")
	cc.startCluster()
	dataDir := filepath.Dir(cc.cluster[0].options.DataDir)
	var wg sync.WaitGroup

	go func() {
		if leader := cc.waitForLeader(); leader != (leaderMap{}) {
			wg.Add(1)
			go cc.submitCommandOnAllNodes(&wg)
		}
	}()

	done, nodeId := false, 0
	for !done {
		<-time.After(10 * time.Second)
		go cc.restartNode(nodeId, &wg)
		if nodeId >= 2 {
			done = true
		}
		nodeId++
	}
	time.Sleep(5 * time.Second)

	go func() {
		if leader := cc.waitForLeader(); leader != (leaderMap{}) {
			wg.Add(1)
			go func() {
				_ = cc.submitCommandOnAllNodes(&wg)
			}()
		}
	}()

	time.AfterFunc(70*time.Second, func() {
		cc.stopCluster(&wg)
	})
	wg.Wait()
	t.Cleanup(func() {
		if shouldBeRemoved(dataDir) {
			_ = os.RemoveAll(dataDir)
		}
		_ = os.Unsetenv("RAFTY_LOG_LEVEL")
	})
}

func shouldBeRemoved(dir string) bool {
	return strings.Contains(filepath.Dir(dir), "rafty")
}
