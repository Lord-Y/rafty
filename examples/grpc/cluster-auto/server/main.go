package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/Lord-Y/rafty"
	"github.com/rs/zerolog/log"
)

var ipAddress = flag.String("ip-address", "127.0.0.5", "ip address")
var clusterSize = flag.Uint("cluster-size", 3, "cluster size")
var maxUptime = flag.Uint("max-uptime", 3, "max uptime in minutes")
var restartNode = flag.Bool("restart-node", false, "restart first node")

type clusterConfig struct {
	ipAddress      string
	maxUptime      uint
	clusterSize    uint
	restartNode    bool
	cluster        []*rafty.Rafty
	TimeMultiplier uint
}

func (cc *clusterConfig) makeCluster() (cluster []*rafty.Rafty) {
	for i := range cc.clusterSize {
		var addr net.TCPAddr

		server := new(rafty.Rafty)
		peers := []*rafty.Peer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: int(rafty.GRPCPort) + int(i),
		}

		for j := range cc.clusterSize {
			var (
				peerAddr string
			)
			if i == j {
				peerAddr = fmt.Sprintf("%s:500%d", *ipAddress, 51+j)
			} else {
				if i > 0 {
					peerAddr = fmt.Sprintf("%s:500%d", *ipAddress, 51+j)
				} else if i > 1 {
					peerAddr = fmt.Sprintf("%s:500%d", *ipAddress, 51+j+i+2)
				} else {
					peerAddr = fmt.Sprintf("%s:500%d", *ipAddress, 51+j+i)
				}
			}

			if addr.String() != peerAddr {
				peers = append(peers, &rafty.Peer{
					Address: peerAddr,
				})

				server = rafty.NewServer(addr)
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
				log.Fatal().Msgf("Fail to start cluster node %d with error %s", i, err.Error())
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
		log.Info().Msgf("Stopping node %d", index)
		node.Stop()
		return nil
	case "restart":
		log.Info().Msgf("Stopping node %d", index)
		node.Stop()
		go func() {
			time.Sleep(1 * time.Second)
			log.Info().Msgf("Restart node %d", index)
			if err := node.Start(); err != nil {
				log.Err(err).Msgf("Fail to start cluster node %d with error %s", index, err.Error())
				return
			}
		}()
		return nil
	default:
		return node.Start()
	}
}

func main() {
	flag.Parse()

	cc := clusterConfig{
		ipAddress:   *ipAddress,
		maxUptime:   *maxUptime,
		clusterSize: *clusterSize,
		restartNode: *restartNode,
	}

	defer func() {
		time.Sleep(time.Duration(cc.maxUptime) * time.Minute)
		cc.stopCluster()
	}()

	if cc.restartNode {
		defer func() {
			time.Sleep(1 * time.Minute)
			cc.startOrStopSpecificicNode(0, "restart")
		}()
	}

	cc.startCluster()
}
