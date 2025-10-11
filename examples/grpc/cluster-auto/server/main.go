package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
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
var snapshotInterval = flag.Uint("snapshot-interval", 30, "Snapshot interval in seconds")
var maxAppendEntries = flag.Uint64("max-appendentries", 60, "Max append entries")

type clusterConfig struct {
	ipAddress                 string
	maxUptime                 uint
	clusterSize               uint
	autoSetMinimumClusterSize bool
	restartNode               bool
	cluster                   []*rafty.Rafty
	TimeMultiplier            uint
	SnapshotInterval          time.Duration
	MaxAppendEntries          uint64
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
					SnapshotInterval:   time.Duration(*snapshotInterval) * time.Second,
					MaxAppendEntries:   *maxAppendEntries,
					IsVoter:            true,
				}
				storeOptions := rafty.BoltOptions{
					DataDir: options.DataDir,
					Options: bolt.DefaultOptions,
				}
				store, _ := rafty.NewBoltStorage(storeOptions)
				fsm := NewSnapshotState(store)
				server, err = rafty.NewRafty(addr, id, options, store, store, fsm, nil)
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

// userLogsInMemory holds the requirements related to user data
type userLogsInMemory struct {
	// mu holds locking mecanism
	mu sync.RWMutex

	// userStore map holds a map of the log entries
	userStore map[uint64]*rafty.LogEntry

	// metadata map holds a map of metadata store
	metadata map[string][]byte

	// kv map holds a map of k/v store
	kv map[string][]byte
}

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore rafty.LogStore

	// userStore is only for user land management
	userStore userLogsInMemory
}

// CommandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type CommandKind uint32

const (
	// commandGet command allows us to fetch data from the cluster
	CommandGet CommandKind = iota

	// CommandSet command allows us to write data from the cluster
	CommandSet

	// CommandDelete command allows us to delete data from the cluster
	CommandDelete
)

// Command is the struct to use to interact with cluster data
type Command struct {
	// Kind represent the set of commands: get, set, del
	Kind CommandKind

	// Key is the name of the key
	Key string

	// Value is the value associated to the key
	Value string
}

// EncodeCommand permits to transform command receive from clients to binary language machine
func EncodeCommand(cmd Command, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(cmd.Kind)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Key))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Key)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cmd.Value))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(cmd.Value)); err != nil {
		return err
	}
	return nil
}

// DecodeCommand permits to transform back command from binary language machine to clients
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	buffer := bytes.NewBuffer(data)

	var kind uint32
	if err := binary.Read(buffer, binary.LittleEndian, &kind); err != nil {
		return cmd, err
	}
	cmd.Kind = CommandKind(kind)

	var keyLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &keyLen); err != nil {
		return cmd, err
	}

	key := make([]byte, keyLen)
	if _, err := buffer.Read(key); err != nil {
		return cmd, err
	}
	cmd.Key = string(key)

	var valueLen uint64
	if err := binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
		return cmd, err
	}
	value := make([]byte, valueLen)
	if _, err := buffer.Read(value); err != nil {
		return cmd, err
	}
	cmd.Value = string(value)

	return cmd, nil
}

// NewSnapshotState return a SnapshotState that allows us to
// take or restore snapshots
func NewSnapshotState(logStore rafty.LogStore) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
		userStore: userLogsInMemory{
			userStore: make(map[uint64]*rafty.LogEntry),
			metadata:  make(map[string][]byte),
			kv:        make(map[string][]byte),
		},
	}
}

// Snapshot allows us to take snapshots
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

// Restore allows us to restore a snapshot
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

// ApplyCommand allows us to apply a command to the state machine.
func (s *SnapshotState) ApplyCommand(log *rafty.LogEntry) ([]byte, error) {
	if log.Command == nil {
		return nil, nil
	}

	decodedCmd, _ := DecodeCommand(log.Command)
	switch decodedCmd.Kind {
	case CommandSet:
		return nil, s.userStore.Set([]byte(decodedCmd.Key), []byte(decodedCmd.Value))

	case CommandGet:
		value, err := s.userStore.Get([]byte(decodedCmd.Key))
		if err != nil {
			return nil, err
		}
		return value, nil

	case CommandDelete:
		return nil, fmt.Errorf("not implemented yet")
	}
	return nil, nil
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *userLogsInMemory) Set(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogsInMemory) Get(key []byte) ([]byte, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return val, nil
	}
	return nil, rafty.ErrKeyNotFound
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *userLogsInMemory) SetUint64(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogsInMemory) GetUint64(key []byte) uint64 {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return rafty.DecodeUint64ToBytes(val)
	}
	return 0
}
