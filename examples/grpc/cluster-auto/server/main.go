package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"math/rand"

	"github.com/Lord-Y/rafty"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
)

var ipAddress = flag.String("ip-address", "127.0.0.5", "ip address")
var clusterSize = flag.Uint("cluster-size", 3, "cluster size")
var autoSetMinimumClusterSize = flag.Bool("auto-set-minimum-cluster-size", false, "auto set minimum cluster size")
var maxUptime = flag.Uint("max-uptime", 3, "max uptime in minutes")
var restartNode = flag.Bool("restart-node", false, "restart first node")

type clusterConfig struct {
	ipAddress                 string
	maxUptime                 uint
	clusterSize               uint
	autoSetMinimumClusterSize bool
	restartNode               bool
	cluster                   []*rafty.Rafty
	TimeMultiplier            uint
}

func (cc *clusterConfig) makeCluster() (cluster []*rafty.Rafty, err error) {
	for i := range cc.clusterSize {
		var addr net.TCPAddr

		server := new(rafty.Rafty)
		initialPeers := []rafty.InitialPeer{}
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: int(rafty.GRPCPort) + int(i),
		}

		for j := range cc.clusterSize {
			var peerAddr string
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
				initialPeers = append(initialPeers, rafty.InitialPeer{
					Address: peerAddr,
				})

				minimumClusterSize := uint64(0)
				if cc.autoSetMinimumClusterSize {
					minimumClusterSize = uint64(cc.clusterSize)
				}
				id := fmt.Sprintf("%d", i)
				options := rafty.Options{
					InitialPeers:       initialPeers,
					DataDir:            filepath.Join(os.TempDir(), "rafty_test", "cluster-auto", id),
					TimeMultiplier:     2,
					MinimumClusterSize: minimumClusterSize,
				}
				storeOptions := rafty.BoltOptions{
					DataDir: options.DataDir,
					Options: bolt.DefaultOptions,
				}
				store, _ := rafty.NewBoltStorage(storeOptions)
				server, err = rafty.NewRafty(addr, id, options, store)
				if err != nil {
					return nil, err
				}
			}
		}
		cluster = append(cluster, server)
	}
	return
}

func (cc *clusterConfig) startCluster() {
	var err error
	cc.cluster, err = cc.makeCluster()
	if err != nil {
		log.Fatal().Msgf("Fail to make the cluster config with error %s", err.Error())
		return
	}

	for i, node := range cc.cluster {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		sleep := 1 + r.Intn(len(cc.cluster))
		time.Sleep(time.Duration(sleep) * time.Second)
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
	now := time.Now()
	flag.Parse()

	cc := clusterConfig{
		ipAddress:                 *ipAddress,
		maxUptime:                 *maxUptime,
		clusterSize:               *clusterSize,
		autoSetMinimumClusterSize: *autoSetMinimumClusterSize,
		restartNode:               *restartNode,
	}

	defer func() {
		time.Sleep(time.Duration(cc.maxUptime) * time.Minute)
		cc.stopCluster()
		cc.cluster[0].Logger.Info().Msgf("cluster has been running for %f minutes", time.Since(now).Minutes())
	}()

	if cc.restartNode {
		defer func() {
			time.Sleep(1 * time.Minute)
			index := 0
			if err := cc.startOrStopSpecificicNode(index, "restart"); err != nil {
				log.Err(err).Msgf("Fail to start cluster node %d with error %s", index, err.Error())
			}
		}()
	}

	go cc.startCluster()
}
