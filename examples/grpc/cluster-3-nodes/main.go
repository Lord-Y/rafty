package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Lord-Y/rafty"
	"github.com/jackc/fake"
	bolt "go.etcd.io/bbolt"
)

var ipAddress = flag.String("ip-address", "127.0.0.1", "ip address")
var maxUptime = flag.Bool("max-uptime", false, "stop node")
var nodeId = flag.Uint("nodeid", 0, "node id config to chose, can be 0, 1, 2 or 3")
var maxUptimeAfterN = flag.Uint("max-uptime-after-n", 6, "max uptime in minutes before stopping node")
var restartNode = flag.Bool("restart-node", false, "restart node")
var restartNodeAfterN = flag.Uint("restart-node-after-n", 2, "max uptime in minutes before restarting node")
var disableNormalMode = flag.Bool("disable-normal-mode", false, "by default the program will run without stopping/restarting so it's needed when using other modes")
var timeMultiplier = flag.Uint("time-multiplier", 0, "TimeMultiplier is a scaling factor that will be used during election timeout")
var submitCommands = flag.Bool("submit-commands", false, "if true submit commands to leader")
var maxCommands = flag.Uint("max-commands", 10, "Max command to submit")
var prevoteDisabled = flag.Bool("prevote-disabled", false, "if true pre vote will be disabled")
var readReplica = flag.Bool("read-replica", false, "only when last node for membership")
var shutdownOnRemove = flag.Bool("shutdown-on-remove", false, "only when last node for membership")

func main() {
	flag.Parse()

	var (
		addr net.TCPAddr
		id   string
	)

	switch *nodeId {
	case 3:
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: 50054,
		}
	case 2:
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: 50053,
		}
	case 1:
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: 50052,
		}
	default:
		addr = net.TCPAddr{
			IP:   net.ParseIP(*ipAddress),
			Port: int(rafty.GRPCPort),
		}
	}

	initialPeers := []rafty.InitialPeer{
		{
			Address: *ipAddress,
		},
		{
			Address: fmt.Sprintf("%s:50052", *ipAddress),
		},
		{
			Address: fmt.Sprintf("%s:50053", *ipAddress),
		},
	}

	id = fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := rafty.Options{
		InitialPeers:    initialPeers,
		DataDir:         filepath.Join(os.TempDir(), "rafty_test", "cluster-3-nodes", id),
		TimeMultiplier:  *timeMultiplier,
		PrevoteDisabled: *prevoteDisabled,
	}
	if *nodeId == 3 {
		if *readReplica {
			options.ReadReplica = true
		}
		if *shutdownOnRemove {
			options.ShutdownOnRemove = true
		}
	}
	storeOptions := rafty.BoltOptions{
		DataDir: options.DataDir,
		Options: bolt.DefaultOptions,
	}
	store, _ := rafty.NewBoltStorage(storeOptions)
	s, _ := rafty.NewRafty(addr, id, options, store)

	if *submitCommands {
		go func() {
			time.AfterFunc(30*time.Second, func() {
				for i := range *maxCommands {
					word := fake.WordsN(5)
					if _, err := s.SubmitCommand(rafty.Command{Kind: rafty.CommandSet, Key: fmt.Sprintf("key_%s_%d", word, i), Value: fmt.Sprintf("value_%s", word)}); err != nil && s.IsRunning() {
						s.Logger.Error().Err(err).Msgf("Fail to submit command %d", i)
						time.Sleep(time.Second)
					}
				}
			})
		}()
	}

	if *disableNormalMode {
		if *restartNodeAfterN >= *maxUptimeAfterN {
			*restartNodeAfterN = 3
		}

		if *maxUptime {
			defer func() {
				time.Sleep(time.Duration(*maxUptimeAfterN) * time.Minute)
				s.Stop()
			}()
		}

		if *restartNode {
			defer func() {
				go func() {
					time.Sleep(time.Duration(*restartNodeAfterN) * time.Minute)
					s.Stop()
					time.Sleep(30 * time.Second)
					s = nil
					var err error
					store, _ := rafty.NewBoltStorage(storeOptions)
					if s, err = rafty.NewRafty(addr, id, options, store); err != nil {
						s.Logger.Fatal().Err(err).Msg("Fail to create cluster config")
					}
					if err := s.Start(); err != nil {
						s.Logger.Fatal().Err(err).Msg("Fail to start node")
					}
				}()
			}()
		}

		go func() {
			if err := s.Start(); err != nil {
				s.Logger.Fatal().Err(err).Msg("Fail to start node")
			}
		}()
		return
	}

	if err := s.Start(); err != nil {
		s.Logger.Fatal().Err(err).Msg("Fail to start node")
	}
}
