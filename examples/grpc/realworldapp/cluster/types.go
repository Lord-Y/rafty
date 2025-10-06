package cluster

import (
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/Lord-Y/rafty"
	"github.com/rs/zerolog"
)

// Cluster hold all required configuration to start the instance
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

	// fsm hold requirements to manipulate store
	fsm *fsmState

	// rafty hold rafty cluster config
	rafty *rafty.Rafty

	// apiServer hold the config of the HTTP API server
	apiServer *http.Server
}

// memoryStore hold the requirements related to user data
type memoryStore struct {
	// mu hold locking mecanism
	mu sync.RWMutex

	// logs map holds a map of the log entries
	logs map[uint64]*rafty.LogEntry

	// kv map holds the a map of k/v store
	kv map[string][]byte

	// users map holds the a map of users store
	users map[string][]byte
}
