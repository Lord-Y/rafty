package main

import (
	"flag"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Lord-Y/rafty"
)

var ipAddress = flag.String("ip-address", "127.0.0.1", "ip address")
var maxUptime = flag.Bool("max-uptime", false, "stop node")
var maxUptimeAfterN = flag.Uint("max-uptime-after-n", 6, "max uptime in minutes before stopping node")
var restartNode = flag.Bool("restart-node", false, "restart node")
var restartNodeAfterN = flag.Uint("restart-node-after-n", 2, "max uptime in minutes before restarting node")
var normalMode = flag.Bool("normal-mode", false, "run prgram normally without stopping/restarting")

func main() {
	flag.Parse()

	addr := net.TCPAddr{
		IP:   net.ParseIP(*ipAddress),
		Port: int(rafty.GRPCPort),
	}
	peers := []rafty.Peer{
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

	s := rafty.NewServer(addr)
	id := "abe35d4f-787e-4262-9894-f6475ed81028"
	s.ID = id
	s.Peers = peers
	s.PersistDataOnDisk = true
	s.DataDir = filepath.Join(os.TempDir(), "rafty", id)

	if *normalMode {
		if err := s.Start(); err != nil {
			s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
		}
		return
	}

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
				if err := s.Start(); err != nil {
					s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
				}
			}()
		}()
	}

	go func() {
		if err := s.Start(); err != nil {
			s.Logger.Fatal().Err(err).Msg("Fail to serve gRPC server")
		}
	}()
}
