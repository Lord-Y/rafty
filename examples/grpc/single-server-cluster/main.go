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
)

var ipAddress = flag.String("ip-address", "127.0.0.1", "ip address")
var maxUptime = flag.Bool("max-uptime", false, "stop node")
var maxUptimeAfterN = flag.Uint("max-uptime-after-n", 6, "max uptime in minutes before stopping node")
var restartNode = flag.Bool("restart-node", false, "restart node")
var restartNodeAfterN = flag.Uint("restart-node-after-n", 2, "max uptime in minutes before restarting node")
var disableNormalMode = flag.Bool("disable-normal-mode", false, "by default the program will run without stopping/restarting so it's needed when using other modes")
var disablePersistance = flag.Bool("disable-persistance", false, "if true persistance will be disabled")
var submitCommands = flag.Bool("submit-commands", false, "if true submit commands to leader")
var maxCommands = flag.Uint("max-commands", 10, "Max command to submit")

func main() {
	flag.Parse()

	addr := net.TCPAddr{
		IP:   net.ParseIP(*ipAddress),
		Port: int(rafty.GRPCPort),
	}

	id := fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := rafty.Options{
		PersistDataOnDisk:     !*disablePersistance,
		DataDir:               filepath.Join(os.TempDir(), "rafty", "single_server"+id),
		IsSingleServerCluster: true,
	}
	s, _ := rafty.NewRafty(addr, id, options)

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
					if s, err = rafty.NewRafty(addr, id, options); err != nil {
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
