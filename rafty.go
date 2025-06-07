package rafty

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	channelzservice "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	// GRPCAddress defines the default address to run the grpc server
	GRPCAddress string = "127.0.0.1"

	// GRPCPort define the default port to run the grpc server
	GRPCPort uint16 = 50051
)

const (
	// electionTimeout is the defaut value used for election campain.
	// In raft paper it's between 150 and 300 milliseconds
	// but we use more for more stability by default
	electionTimeout int = 500

	// heartbeatTimeout is the maximum time a follower will used to detect if there is a leader.
	// If no leader, a new election campain will be started
	heartbeatTimeout int = 500

	// maxAppendEntries will hold how much append entries the leader will send to the follower at once
	maxAppendEntries uint64 = 1000
)

type Peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr
}

type leaderMap struct {
	// address is the address of a peer node with explicit host and port
	address string

	// id of the current peer
	id string
}

type rpcManager struct {
	raftypb.RaftyServer

	// rafty holds rafty config
	rafty *Rafty
}

// Options hold config that will be modified by users
type Options struct {
	// logSource is only use during unit testing running in parallel in order to
	// better debug logs
	logSource string

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	// ElectionTimeout is used to start new election campain.
	// Its value will be divided by 2 in order to randomize election timing process.
	// It must be greater or equal to HeartbeatTimeout.
	// Unit is in milliseconds
	ElectionTimeout int

	// HeartbeatTimeout is used by follower without contact from the leader
	// before starting new election campain.
	// Unit is in milliseconds
	HeartbeatTimeout int

	// TimeMultiplier is a scaling factor that will be used during election timeout
	// by electionTimeoutMin/electionTimeoutMax/leaderHeartBeatTimeout in order to avoid cluster instability
	// The default value is 1 and the maximum is 10
	TimeMultiplier uint

	// MinimumClusterSize is the size minimum to have before starting prevote or election campain
	// default is 3
	// all members of the cluster will be contacted before any other tasks
	MinimumClusterSize uint64

	// MaxAppendEntries will hold how much append entries the leader will send to the follower at once
	MaxAppendEntries uint64

	// DataDir is the default data directory that will be used to store all data on the disk
	// Defaults to os.TempDir()/rafty ex: /tmp/rafty
	DataDir string

	// PersistDataOnDisk is a boolean that allow us to persist data on disk
	PersistDataOnDisk bool

	// ReadOnlyNode allow to statuate if the current node is a read only node
	// This kind of node won't participate into any election campain
	ReadOnlyNode bool

	// Peers hold the list of the peers
	Peers []Peer

	// Disable pre vote is a boolean the allow us to directly start
	// vote election without pre vote step
	DisablePrevote bool
}

// Rafty is a struct representing the raft requirements
type Rafty struct {
	wg sync.WaitGroup

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// murw will be mostly used with map to avoid data races
	murw sync.RWMutex

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	// options are configuration options
	options Options

	// id of the current raft server
	id string

	// Address is the current address of the raft server
	Address net.TCPAddr

	// State of the current raft server state
	// Can only be Leader, Candidate, Follower, ReadOnly, Down
	State

	// grpc listener
	listener net.Listener

	// grpcServer hold requirements for grpc server
	grpcServer *grpc.Server

	// timer is used during the election campain or heartbeat.
	// It will be used to detect if a Follower
	// need to step up as a Candidate to then becoming a Leader
	timer *time.Ticker

	// rpcPreVoteRequestChan will be used to handle rpc call
	rpcPreVoteRequestChan chan preVoteResquestWrapper

	// rpcVoteRequestChan will be used to handle rpc call
	rpcVoteRequestChan chan voteResquestWrapper

	// rpcSendAppendEntriesRequestChan will be used to handle rpc call
	rpcAppendEntriesRequestChan chan appendEntriesResquestWrapper

	// triggerAppendEntriesChan is the chan that will trigger append entries
	// without waiting leader hearbeat append entries
	triggerAppendEntriesChan chan triggerAppendEntries

	// logOperationChan is the chan that will be used
	// to read or write logs safely in memory
	logOperationChan chan logOperationRequest

	// rpcForwardCommandToLeaderRequestChan will be used to handle rpc client call to leader
	rpcForwardCommandToLeaderRequestChan chan forwardCommandToLeaderRequestWrapper

	// rpcAskNodeIDChan will be used to handle rpc call
	rpcAskNodeIDChan chan RPCResponse

	// rpcClientGetLeaderChan will be used to handle rpc call
	rpcClientGetLeaderChan chan RPCResponse

	// leaderFound is only related to rpc call GetLeader
	leaderFound atomic.Bool

	// leaderCount is only related to rpc call GetLeader
	leaderCount atomic.Uint64

	// leaderLost is a boolean that allow the node to properly
	// restart pre election campain when leader is lost
	leaderLost atomic.Bool

	// leader hold informations about the leader
	leader sync.Map

	// leaderLastContactDate is the last date we heard the leader
	leaderLastContactDate atomic.Value

	// startElectionCampain permit to start election campain as
	// pre vote quorum as been reached
	startElectionCampain atomic.Bool

	// quitCtx will be used to shutdown the server
	quitCtx context.Context

	// stopCtx is used with quitCtx to shutdown the server
	stopCtx context.CancelFunc

	// isRunning is a helper indicating is the node is up or down.
	// It set to false, it will reject all incoming grpc requests
	// with shutting down error
	isRunning atomic.Bool

	// minimumClusterSizeReach is an atomic bool flag to set
	// and start follower requirements
	minimumClusterSizeReach atomic.Bool

	// clusterSizeCounter is used to check how many nodes has been reached
	// before acknoledging the start prevote election
	clusterSizeCounter atomic.Uint64

	// votedFor is the node the current node voted for during the election campain
	votedFor string

	// votedForTerm is the node the current node voted for during the election campain
	votedForTerm atomic.Uint64

	// currentTerm is latest term seen during the voting campain
	currentTerm atomic.Uint64

	// lastApplied is the index of the highest log entry applied to the current raft server
	lastApplied atomic.Uint64

	// lastAppliedConfig is the index of the highest log entry configuration applied
	// to the current raft server
	lastAppliedConfig atomic.Uint64

	// commitIndex is the highest log entry known to be committed
	// initialized to 0, increases monotically
	commitIndex atomic.Uint64

	// lastLogIndex is the highest log entry applied to state machine
	// initialized to 0, increases monotically
	lastLogIndex atomic.Uint64

	// lastLogTerm is the highest log term linked to lastLogIndex in the state machine
	// initialized to 0, increases monotically
	lastLogTerm atomic.Uint64

	// nextIndex is for each server, index of the next log entry
	// to send to that server
	// initialized to leader last log index + 1
	nextIndex atomic.Uint64

	// matchIndex is for each server, index of the highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotically
	matchIndex atomic.Uint64

	// logs allow us to manipulate logs
	logs logs

	// configuration hold server members found on disk
	// If empty, it will be equal to Peers list
	//
	// When a new member has been found into Peers list and not on disk
	// a cluster membership will be initiated in order to add it
	//
	// When persistant storage is not enabled and the cluster start with 3 nodes
	// if a new node is started it won't be part of the initial cluster list
	// so a cluster membership will be initiated in order to add it
	configuration configuration

	// connectionManager hold connections for all members
	connectionManager connectionManager

	// storage hold requirements to store/restore logs and metadata
	storage storage
}

// preVoteResquestWrapper is a struct that will be used to send response to the caller
type preVoteResquestWrapper struct {
	// request of the peer
	request      *raftypb.PreVoteRequest
	responseChan chan *raftypb.PreVoteResponse
}

// voteResquestWrapper is a struct that will be used to send response to the caller
type voteResquestWrapper struct {
	// request of the peer
	request *raftypb.VoteRequest

	// responseChan will be used to send back the response
	responseChan chan *raftypb.VoteResponse
}

// appendEntriesResquestWrapper is a struct that will be used to send response to the caller
type appendEntriesResquestWrapper struct {
	// request of the peer
	request *raftypb.AppendEntryRequest

	// responseChan will be used to send back the response
	responseChan chan *raftypb.AppendEntryResponse
}

// forwardCommandToLeaderRequestWrapper is a struct that will be used to send response to the caller
type forwardCommandToLeaderRequestWrapper struct {
	// request of the peer
	request *raftypb.ForwardCommandToLeaderRequest

	// responseChan will be used to send back the response
	responseChan chan *raftypb.ForwardCommandToLeaderResponse
}

// NewRafty instantiate rafty with default configuration
// with server address and its id
func NewRafty(address net.TCPAddr, id string, options Options) *Rafty {
	r := &Rafty{
		rpcPreVoteRequestChan:                make(chan preVoteResquestWrapper),
		rpcVoteRequestChan:                   make(chan voteResquestWrapper),
		rpcAppendEntriesRequestChan:          make(chan appendEntriesResquestWrapper),
		triggerAppendEntriesChan:             make(chan triggerAppendEntries),
		logOperationChan:                     make(chan logOperationRequest),
		rpcForwardCommandToLeaderRequestChan: make(chan forwardCommandToLeaderRequestWrapper),
		rpcAskNodeIDChan:                     make(chan RPCResponse),
		rpcClientGetLeaderChan:               make(chan RPCResponse),
	}
	r.Address = address
	r.id = id

	if options.Logger == nil {
		var zlogger zerolog.Logger
		if options.logSource == "" {
			zlogger = logger.NewLogger().With().Str("logProvider", "rafty").Logger()
		} else {
			zlogger = logger.NewLogger().With().Str("logProvider", "rafty").Str("logSource", options.logSource).Logger()
		}
		options.Logger = &zlogger
	}

	if options.TimeMultiplier == 0 {
		options.TimeMultiplier = 1
	}
	if options.TimeMultiplier > 10 {
		options.TimeMultiplier = 10
	}

	if options.MinimumClusterSize == 0 {
		options.MinimumClusterSize = 3
	}

	if options.MaxAppendEntries == 0 {
		options.MaxAppendEntries = maxAppendEntries
	}

	if options.MaxAppendEntries > maxAppendEntries {
		options.MaxAppendEntries = maxAppendEntries
	}

	if options.ElectionTimeout < electionTimeout {
		options.ElectionTimeout = electionTimeout
	}

	if options.HeartbeatTimeout < heartbeatTimeout {
		options.HeartbeatTimeout = heartbeatTimeout
	}

	if options.ElectionTimeout < options.HeartbeatTimeout {
		options.ElectionTimeout, options.HeartbeatTimeout = options.HeartbeatTimeout, options.ElectionTimeout
	}

	if options.DataDir == "" {
		options.DataDir = filepath.Join(os.TempDir(), "rafty")
	}

	r.options = options
	r.Logger = options.Logger

	r.connectionManager = connectionManager{
		id:          id,
		address:     address.String(),
		logger:      options.Logger,
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]raftypb.RaftyClient),
	}

	metaFile, dataFile := r.newStorage()
	r.storage = storage{
		metadata: metaFile,
		data:     dataFile,
	}
	r.storage.metadata.rafty = r
	r.storage.data.rafty = r

	r.logs = r.newLogs()
	return r
}

// Start permits to start the node with the provided configuration
func (r *Rafty) Start() error {
	var err error

	if err = r.storage.restore(); err != nil {
		return fmt.Errorf("fail to restore data %w", err)
	}

	if err = r.parsePeers(); err != nil {
		return fmt.Errorf("fail to parse peer ip/port %w", err)
	}

	if r.id == "" {
		r.id = uuid.NewString()
		r.connectionManager.id = r.id
		if err := r.storage.metadata.store(); err != nil {
			return fmt.Errorf("fail to persist metadata %w", err)
		}
	}

	r.mu.Lock()
	if r.listener, err = net.Listen(r.Address.Network(), r.Address.String()); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("fail to listen gRPC server %w", err)
	}

	r.grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	rpcManager := rpcManager{
		rafty: r,
	}
	r.mu.Unlock()

	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(r.grpcServer, healthcheck)
	raftypb.RegisterRaftyServer(r.grpcServer, &rpcManager)
	reflection.Register(r.grpcServer)
	channelzservice.RegisterChannelzServiceToServer(r.grpcServer)

	r.mu.Lock()
	r.quitCtx, r.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	r.mu.Unlock()
	defer r.stopCtx()

	r.wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		defer r.wg.Done()
		errChan <- r.grpcServer.Serve(r.listener)
	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("fail to start gRPC server %w", err)
	default:
	}

	if r.getState() == Down {
		if r.options.ReadOnlyNode {
			r.switchState(ReadOnly, stepUp, false, r.currentTerm.Load())
		} else {
			r.switchState(Follower, stepUp, false, r.currentTerm.Load())
		}
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Node successfully started")
	}

	r.wg.Add(1)
	go r.start()
	r.isRunning.Store(true)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// stop go routine when os signal is receive or ctrl+c
		<-r.quitCtx.Done()
		r.stop()
	}()
	r.wg.Wait()
	return nil
}

// start run sub requirements from Start() func
func (r *Rafty) start() {
	defer r.wg.Done()

	r.wg.Add(1)
	go r.commonLoop()
	if !r.checkNodeIDs() {
		r.sendAskNodeIDRequest()
		r.startClusterWithMinimumSize()
	}

	r.wg.Add(1)
	go r.logsLoop()
	r.sendGetLeaderRequest()
	r.stateLoop()
}

// Stop permits to stop the gRPC server by using signal
func (r *Rafty) Stop() {
	// if statement is need otherwise some tests panics
	if r.isRunning.Load() {
		r.stopCtx()
	}
}

// stop permits to stop the gRPC server and Rafty with the provided configuration
func (r *Rafty) stop() {
	r.isRunning.Store(false)
	// this is just a safe guard when invoking Stop function directly
	r.switchState(Down, stepDown, true, r.currentTerm.Load())
	r.release()

	timer := time.AfterFunc(60*time.Second, func() {
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Node couldn't stop gracefully in time, doing force stop")
		r.grpcServer.Stop()
	})
	defer timer.Stop()
	r.grpcServer.GracefulStop()
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Node successfully stopped with term %d", r.currentTerm.Load())
	r.storage.close()
}

// checkNodeIDs check if we gather all node ids.
// If it is, we will check if there is a leader
// otherwise we will wait to fetch them all.
// If we are restarting the node, we normally already have all nodes ids
// so we won't go further
func (r *Rafty) checkNodeIDs() bool {
	peers, count := r.getPeers()

	for _, peer := range peers {
		if peer.ID != "" {
			count--
			if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !peer.ReadOnlyNode {
				r.clusterSizeCounter.Add(1)
			}
		}
	}

	if count == 0 {
		r.minimumClusterSizeReach.Store(true)
		return true
	}
	return false
}

// startClusterWithMinimumSize allow us to reach minimum cluster size
// before doing anything else
func (r *Rafty) startClusterWithMinimumSize() {
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	for r.getState() != Down && !r.minimumClusterSizeReach.Load() {
		select {
		case <-r.quitCtx.Done():
			return

		case <-timer.C:
			if r.options.MinimumClusterSize == r.clusterSizeCounter.Load()+1 {
				r.minimumClusterSizeReach.Store(true)
				r.Logger.Info().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Msgf("Minimum cluster size has been reached: %d out of %d", r.clusterSizeCounter.Load()+1, r.options.MinimumClusterSize)
				return
			}
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("clusterSize", fmt.Sprintf("%d", r.clusterSizeCounter.Load()+1)).
				Msgf("Cluster size not reached")
			go r.sendAskNodeIDRequest()
		}
	}
}
