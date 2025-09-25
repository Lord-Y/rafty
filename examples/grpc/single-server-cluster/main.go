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
	"sync"
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
	cacheOptions := rafty.LogCacheOptions{
		LogStore:     store,
		CacheOnWrite: true,
		TTL:          2 * time.Second,
	}
	cacheStore := rafty.NewLogCache(cacheOptions)

	fsm := NewSnapshotState(store)
	s, err := rafty.NewRafty(addr, id, options, cacheStore, store, fsm, nil)
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
					buffer := new(bytes.Buffer)
					if err := EncodeCommand(Command{Kind: CommandSet, Key: fmt.Sprintf("key_%s_%d", word, i), Value: fmt.Sprintf("value_%s", word)}, buffer); err != nil {
						s.Logger.Fatal().Err(err).Msgf("Fail to encode command %d", i)
					}
					if _, err := s.SubmitCommand(0, rafty.LogReplication, buffer.Bytes()); err != nil && s.IsRunning() {
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
					cacheOptions := rafty.LogCacheOptions{
						LogStore:     store,
						CacheOnWrite: true,
						TTL:          2 * time.Second,
					}
					cacheStore := rafty.NewLogCache(cacheOptions)
					fsm := NewSnapshotState(cacheStore)
					if s, err = rafty.NewRafty(addr, id, options, cacheStore, store, fsm, nil); err != nil {
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

// userLogInMemory hold the requirements related to user data
type userLogInMemory struct {
	// mu hold locking mecanism
	mu sync.RWMutex

	// userStore map holds a map of the log entries
	userStore map[uint64]*rafty.LogEntry

	// metadata map holds the a map of metadata store
	metadata map[string][]byte

	// kv map holds the a map of k/v store
	kv map[string][]byte
}

// SnapshotState is a struct holding a set of configs needed to take a snapshot.
// This can be used by fsm (finite state machine) as an example.
// See state_machine_test.go
type SnapshotState struct {
	// LogStore is the store holding the data
	logStore rafty.LogStore

	// userStore is only for user land management
	userStore userLogInMemory
}

// CommandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type CommandKind uint32

const (
	// commandGet command allow us to fetch data from the cluster
	CommandGet CommandKind = iota

	// CommandSet command allow us to write data from the cluster
	CommandSet

	// CommandDelete command allow us to delete data from the cluster
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

// NewSnapshotState return a SnapshotState that allow us to
// take or restore snapshots
func NewSnapshotState(logStore rafty.LogStore) *SnapshotState {
	return &SnapshotState{
		logStore: logStore,
		userStore: userLogInMemory{
			userStore: make(map[uint64]*rafty.LogEntry),
			metadata:  make(map[string][]byte),
			kv:        make(map[string][]byte),
		},
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

// ApplyCommand allow us to apply a command to the state machine.
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
func (in *userLogInMemory) Set(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogInMemory) Get(key []byte) ([]byte, error) {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return val, nil
	}
	return nil, rafty.ErrKeyNotFound
}

// Set will add key/value to the k/v store.
// An error will be returned if necessary
func (in *userLogInMemory) SetUint64(key, value []byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()

	in.kv[string(key)] = value
	return nil
}

// Get will fetch provided key from the k/v store.
// An error will be returned if the key is not found
func (in *userLogInMemory) GetUint64(key []byte) uint64 {
	in.mu.RLock()
	defer in.mu.RUnlock()

	if val, ok := in.kv[string(key)]; ok {
		return rafty.DecodeUint64ToBytes(val)
	}
	return 0
}
