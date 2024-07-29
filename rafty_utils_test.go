package rafty

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterConfig struct {
	t              *testing.T
	clusterSize    uint
	cluster        []*Rafty
	TimeMultiplier uint
}

func (cc *clusterConfig) makeCluster() (cluster []*Rafty) {
	for i := range cc.clusterSize {
		var addr net.TCPAddr

		server := new(Rafty)
		peers := []*Peer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP("127.0.0.5"),
			Port: int(GRPCPort) + int(i),
		}

		for j := range cc.clusterSize {
			var (
				peerAddr string
			)
			if i == j {
				peerAddr = fmt.Sprintf("127.0.0.5:500%d", 51+j)
			} else {
				if i > 0 {
					peerAddr = fmt.Sprintf("127.0.0.5:500%d", 51+j)
				} else if i > 1 {
					peerAddr = fmt.Sprintf("127.0.0.5:500%d", 51+j+i+2)
				} else {
					peerAddr = fmt.Sprintf("127.0.0.5:500%d", 51+j+i)
				}
			}

			if addr.String() != peerAddr {
				peers = append(peers, &Peer{
					Address: peerAddr,
				})

				server = NewServer(addr)
				server.ID = fmt.Sprintf("%d", i)
				server.Peers = peers
				server.TimeMultiplier = 2
			}
		}
		cluster = append(cluster, server)
	}
	return
}

func (cc *clusterConfig) startCluster() {
	cc.cluster = cc.makeCluster()
	for i, node := range cc.cluster {
		go func() {
			if err := node.Start(); err != nil {
				cc.t.Errorf("Fail to start cluster node %d with error %s", i, err.Error())
				return
			}
		}()
	}
}

func (cc *clusterConfig) stopCluster() {
	for _, node := range cc.cluster {
		node.Stop()
	}
}

func (cc *clusterConfig) startOrStopSpecificicNode(index int, action string) error {
	node := cc.cluster[index]
	switch action {
	case "stop":
		node.Logger.Info().Msgf("Stopping node %d", index)
		node.Stop()
		return nil
	case "restart":
		node.Logger.Info().Msgf("Stopping node %d", index)
		node.Stop()
		node.Logger.Info().Msgf("Stopped node %d", index)
		time.Sleep(3 * time.Second)
		go func() {
			node.Logger.Info().Msgf("Restart node %d", index)
			if err := node.Start(); err != nil {
				node.Logger.Info().Msgf("Error while starting node %d with error %s", index, err.Error())
				cc.t.Errorf("Fail to start cluster node %d with error %s", index, err.Error())
			}
		}()
		return nil
	default:
		node.Logger.Info().Msgf("Start node %d", index)
		return node.Start()
	}
}

func (cc *clusterConfig) clientGetLeader() bool {
	assert := assert.New(cc.t)
	nodeAddr := cc.cluster[0].Address.String()
	conn, err := grpc.NewClient(
		nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		assert.Errorf(err, "Fail to connect to grpc server %s", nodeAddr)
	}
	defer conn.Close()
	client := grpcrequests.NewRaftyClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := client.ClientGetLeader(ctx, &grpcrequests.ClientGetLeaderRequest{Message: "Who is the leader?"})
	if err != nil {
		assert.Errorf(err, "Fail to ask %s who is the leader", nodeAddr)
	}
	if response.GetLeaderAddress() == "" && response.GetLeaderID() == "" {
		cc.cluster[0].Logger.Info().Msgf("No leader found")
		return false
	}
	cc.cluster[0].Logger.Info().Msgf("%s / %s is the leader", response.GetLeaderAddress(), response.GetLeaderID())
	return true
}
