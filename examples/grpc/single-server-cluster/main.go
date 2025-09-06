package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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
var maxUptimeAfterN = flag.Uint("max-uptime-after-n", 6, "max uptime in minutes before stopping node")
var restartNode = flag.Bool("restart-node", false, "restart node")
var restartNodeAfterN = flag.Uint("restart-node-after-n", 2, "max uptime in minutes before restarting node")
var disableNormalMode = flag.Bool("disable-normal-mode", false, "by default the program will run without stopping/restarting so it's needed when using other modes")
var submitCommands = flag.Bool("submit-commands", false, "if true submit commands to leader")
var maxCommands = flag.Uint("max-commands", 500, "Max command to submit")
var snapshotInterval = flag.Uint("snapshot-interval", 30, "Snapshot interval in seconds")

func main() {
	flag.Parse()

	addr := net.TCPAddr{
		IP:   net.ParseIP(*ipAddress),
		Port: int(rafty.GRPCPort),
	}

	id := fmt.Sprintf("%d", addr.Port)
	id = id[len(id)-2:]
	options := rafty.Options{
		DataDir:               filepath.Join(os.TempDir(), "rafty_test", "single_server"+id),
		IsSingleServerCluster: true,
		SnapshotInterval:      time.Duration(*snapshotInterval) * time.Second,
	}
	storeOptions := rafty.BoltOptions{
		DataDir: options.DataDir,
		Options: bolt.DefaultOptions,
	}
	store, err := rafty.NewBoltStorage(storeOptions)
	if err != nil {
		log.Fatal(err)
	}
	fsm := NewSnapshotState(store)
	s, err := rafty.NewRafty(addr, id, options, store, fsm, nil)
	if err != nil {
		log.Fatal(err)
	}
	s.Logger.Info().
		Str("address", s.Address.String()).
		Str("id", id).
		Msgf("data dir %s", options.DataDir)

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
					store, err := rafty.NewBoltStorage(storeOptions)
					if err != nil {
						s.Logger.Fatal().Err(err).Msg("Fail to create sotre")
					}
					fsm := NewSnapshotState(store)
					if s, err = rafty.NewRafty(addr, id, options, store, fsm, nil); err != nil {
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

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore rafty.Store
}

// NewSnapshotState return a SnapshotState that allow us to
// take or restore snapshots
func NewSnapshotState(logStore rafty.Store) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
	}
}

// Snapshot allow us to take snapshots
func (s *SnapshotState) Snapshot(snapshotWriter io.Writer) error {
	var err error
	firstIndex, err := s.logStore.FirstIndex()
	if err != nil && !errors.Is(err, rafty.ErrKeyNotFound) {
		return err
	}

	lastIndex, err := s.logStore.LastIndex()
	if err != nil && !errors.Is(err, rafty.ErrKeyNotFound) {
		return err
	}

	if firstIndex != lastIndex {
		// 64 here will do nothing except telling us a snapshot is needed
		// which we are building
		response := s.logStore.GetLogsByRange(firstIndex, lastIndex, 64)
		if response.Err != nil {
			return err
		}
		for _, logEntry := range response.Logs {
			var err error
			buffer, bufferChecksum := new(bytes.Buffer), new(bytes.Buffer)
			if err = rafty.MarshalBinary(logEntry, buffer); err != nil {
				return err
			}
			if err = rafty.MarshalBinaryWithChecksum(buffer, bufferChecksum); err != nil {
				return err
			}
			// writting data to the file handler
			if _, err = snapshotWriter.Write(bufferChecksum.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// Restore allow us to restore a snapshot
func (s *SnapshotState) Restore(snapshotReader io.Reader) error {
	var logs []*rafty.LogEntry
	reader := bufio.NewReader(snapshotReader)

	for {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read first 4 bytes to get entry size
		record := make([]byte, length)
		if _, err := io.ReadFull(reader, record); err != nil {
			return err
		}

		data, err := rafty.UnmarshalBinaryWithChecksum(record)
		if err != nil {
			return err
		}

		if data != nil {
			logs = append(logs, data)
		}
	}

	return s.logStore.StoreLogs(logs)
}
