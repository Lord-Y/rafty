package rafty

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterConfig struct {
	t                         *testing.T
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
}

func (cc *clusterConfig) makeCluster() (cluster []*Rafty) {
	defaultPort := int(GRPCPort)
	if cc.portStartRange == 0 {
		cc.portStartRange = 50000
	} else {
		defaultPort = int(cc.portStartRange) + 51
	}
	for i := range cc.clusterSize {
		var addr net.TCPAddr

		server := new(Rafty)
		peers := []Peer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP("127.0.0.5"),
			Port: defaultPort + int(i),
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

				server = NewServer(addr)
				if !cc.noNodeID {
					server.ID = fmt.Sprintf("%d", i)
				}
				server.Peers = peers
			}

			server.TimeMultiplier = cc.timeMultiplier
			if cc.autoSetMinimumClusterSize {
				server.MinimumClusterSize = uint64(cc.clusterSize)
			}
			server.MaxAppendEntries = cc.maxAppendEntries
			if cc.noDataDir && i != 0 || !cc.noDataDir {
				server.PersistDataOnDisk = true
				server.DataDir = filepath.Join(os.TempDir(), "rafty_test", cc.testName, fmt.Sprintf("node%d", i))
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
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			sleep := 1 + r.Intn(10)
			go func() {
				time.Sleep(time.Duration(sleep) * time.Second)
				if err := node.Start(); err != nil {
					t.Errorf("Fail to start cluster node %d with error %s", i, err.Error())
					return
				}
			}()
		})
	}
}

func (cc *clusterConfig) stopCluster() {
	for _, node := range cc.cluster {
		node.Stop()
	}
}

func (cc *clusterConfig) startOrStopSpecificicNode(nodeId int, action string) error {
	node := cc.cluster[nodeId]
	switch action {
	case "stop":
		node.Logger.Info().Msgf("Stopping node %d", nodeId)
		node.Stop()
		return nil
	case "restart":
		node.Logger.Info().Msgf("Stopping node %d", nodeId)
		node.Stop()
		node.Logger.Info().Msgf("Stopped node %d", nodeId)
		for i := 0; i < 100; i++ {
			time.Sleep(5 * time.Second)
			node.Logger.Info().Msgf("Sleeping number %d waiting for node %d to be completely stopped", i, nodeId)
			if node.getState() == Down {
				time.Sleep(3 * time.Second)
				node = nil
				redoCluster := cc.makeCluster()
				node = redoCluster[nodeId]
				go func() {
					node.Logger.Info().Msgf("Restart node %d", nodeId)
					if err := node.Start(); err != nil {
						node.Logger.Info().Msgf("Error while starting node %d with error %s", nodeId, err.Error())
						cc.t.Errorf("Fail to start cluster node %d with error %s", nodeId, err.Error())
					}
				}()
				break
			}
		}
		return nil
	default:
		node.Logger.Info().Msgf("Start node %d", nodeId)
		return node.Start()
	}
}

func (cc *clusterConfig) clientGetLeader(nodeId int) (bool, string, string) {
	assert := assert.New(cc.t)
	nodeAddr := cc.cluster[0].Address.String()
	conn, err := grpc.NewClient(
		nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		assert.Errorf(err, "Node %d reports fail to connect to grpc server %s", nodeId, nodeAddr)
	}
	defer conn.Close()
	client := raftypb.NewRaftyClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := client.ClientGetLeader(ctx, &raftypb.ClientGetLeaderRequest{Message: "Who is the leader?"})
	if err != nil {
		assert.Errorf(err, "Node %d reports fail to ask %s who is the leader", nodeId, nodeAddr)
	}
	if response.GetLeaderAddress() == "" && response.GetLeaderID() == "" {
		cc.cluster[nodeId].Logger.Info().Msgf("Node %d reports no leader found", nodeId)
		return false, response.GetLeaderAddress(), response.GetLeaderID()
	}
	cc.cluster[nodeId].Logger.Info().Msgf("Node %d reports that %s / %s is the leader", nodeId, response.GetLeaderAddress(), response.GetLeaderID())
	return true, "", ""
}

func (cc *clusterConfig) testClustering(t *testing.T) {
	logSource = cc.testName
	if cc.runTestInParallel {
		t.Parallel()
	}
	assert := assert.New(t)

	submitCommandToNode := func(nodeId int) {
		time.Sleep(20 * time.Second)
		var found bool
		for !found {
			<-time.After(time.Second)
			found, _, _ = cc.clientGetLeader(nodeId)
			if found {
				for i, node := range cc.cluster {
					cc.t.Run(fmt.Sprintf("%s_submitCommandToNode_%d_%d", cc.testName, nodeId, i), func(t *testing.T) {
						_, err := node.SubmitCommand(command{kind: commandSet, key: fmt.Sprintf("key%d%d", nodeId, i), value: fmt.Sprintf("value%d", i)})
						if err != nil {
							switch {
							case strings.Contains(err.Error(), "the client connection is closing"):
								assert.Contains(err.Error(), "the client connection is closing")
							case strings.Contains(err.Error(), "CommandNotFound"):
								assert.Equal(errCommandNotFound, err)
							case strings.Contains(err.Error(), "NoLeader"):
								assert.Equal(errNoLeader, err)
							case strings.Contains(err.Error(), "Fail to forward command to leader"):
								assert.Contains(err.Error(), "Fail to forward command to leader")
							default:
								assert.Equal(fmt.Errorf("NoLeader"), err)
							}
						} else {
							assert.Nil(err)
						}
					})
				}
			}
		}
	}

	os.Setenv("RAFTY_LOG_LEVEL", "trace")
	cc.startCluster()
	time.Sleep(2 * time.Second)

	startAndRestart := func(node int) {
		cc.t.Run(fmt.Sprintf("startAndRestart_%d", node), func(t *testing.T) {
			err := cc.startOrStopSpecificicNode(node, "start")
			cc.cluster[node].Logger.Info().Msgf("err startOrStopSpecificicNode start %s", err.Error())
			assert.Error(err)

			time.Sleep(5 * time.Second)
			err = cc.startOrStopSpecificicNode(node, "restart")
			if err != nil {
				cc.cluster[node].Logger.Info().Msgf("err startOrStopSpecificicNode restart %s", err.Error())
			}
			assert.Nil(err)
		})
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

	time.Sleep(150 * time.Second)
	os.Unsetenv("RAFTY_LOG_LEVEL")
	cc.stopCluster()
	err := os.RemoveAll(cc.cluster[node].DataDir)
	assert.Nil(err)
}
