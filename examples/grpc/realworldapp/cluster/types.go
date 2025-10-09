package cluster

import (
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/Lord-Y/rafty"
	"github.com/rs/zerolog"
)

// Cluster holds all required configuration to start the instance
type Cluster struct {
	Logger *zerolog.Logger

	// host is the address to use by the cluster
	Host string

	// HTTPPort to use to handle http requests
	HTTPPort int

	// GRPCPort to use for rafty cluster
	GRPCPort int

	// Members are the rafty cluster members
	Members []string

	// address is the address of the current node
	address net.TCPAddr

	// id is the id of the current node
	id string

	// dataDir is the working directory of this node
	dataDir string

	quit chan os.Signal

	// fsm holds requirements to manipulate store
	fsm *fsmState

	// rafty holds rafty cluster config
	rafty *rafty.Rafty

	// apiServer holds the config of the HTTP API server
	apiServer *http.Server
}

// data holds informations about the decoded command and its log index.
// this index is used to know what entry to drop or override
type data struct {
	// index is the index of the log entry
	index uint64

	// value is the value of the command
	value []byte
}

// memoryStore holds the requirements related to user data
type memoryStore struct {
	// mu holds locking mecanism
	mu sync.RWMutex

	// logs map holds a map of the log entries
	logs map[uint64]*rafty.LogEntry

	// kv map holds a map of decoded k/v store
	kv map[string]data

	// users map holds a map of decoded users store
	users map[string]data
}
