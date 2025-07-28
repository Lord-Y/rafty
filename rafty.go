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

/*

How our raft cluster implementation works? It diverge slighly from raft paper by adding new safegards.
When a 3 nodes cluster start with 0 logs, all nodes will contact each other to know who is the leader
and if a node is part of the cluster, they will exchange theirs ids. Without getting all ids,
no further steps will be done like election campaign.
This also means that if nth nodes out of 3 is not up and running, no further process will be done.
On the opposite side, if all 3 nodes were running and one of them shut down, when restarting, it will do
business as usual it already has all ids.

ADDING A NODE
Adding a new node is NOT exposed to devops for cluster management.
If the 4th node contact the cluster, it will:
- ask to all members who is the leader and will keep asking when not found by any actual members of the cluster
- the contacted node will provide the leader informations AND will also tell it to ask for membership
  as the new node is NOT part of the cluster
- contact the leader for membership
- the leader will start replicate its logs in nth rounds with the informations provided when asked for membership
- if the leader determined the the new node logs are up to date enough, a log entry config will be appended to
  the logs and replicated to followers. In this log entry config, the flag WaitToBePromoted will be set to true.
	That means the current cluster WILL NEVER start election campaign with this node if the actual
	leader crashes. If the actual leader crashes, the new node will restart the membership process from the beginning.
	Once the leader knows that the new log entry config has been committed, it will ONLY THEN promote the node.
	Of course if the node is TOO SLOW or something got wrong, it won't be promoted AND will restart membership process again.
	To promote the new node, another log entry config will created BUT with WaitToBePromoted flag set to false and
	sent for replication. The leader will also add the new node configuration into the replication process in order to received
	append entries from the leader like any other followers.
	Again if the leader crashes during the promotion process, no worries, the new node will restart membership process.
	A node with WaitToBePromoted set to true can be safely removed or force removed

DEMOTING A NODE
Demoting a node is exposed to devops for cluster management.
Demoting a node MUST be used when a it needs to be stopped for maintenance for example.
A log config entry will be created AND Decommissioning will be set to true.
The cluster WILL NEVER start election campaign with this node if the actual leader crashes.
The leader will keep sending append entries to it but will not be counted into the quorum.
The leader will check if the node can be demoted WITHOUT breaking the cluster. If it will, an error will be returned.
A demoted node can be promoted back.
A demoted node can be then removed from the cluster, see REMOVE section.
Somehow if both WaitToBePromoted AND Decommissioning are set to true the leader WON'T send any append entries to it.

PROMOTING A NODE
Promoting a node is exposed to devops for cluster management.
Promoting a node can be used to promote back a demoted node.
It is also used when a new node is added into the cluster, see ADDING A NODE section
Somehow if both WaitToBePromoted AND Decommissioning are set to true, promoting the node will
only will set Decommissioning to false.
The node will then retry the membership process to be promoted by the leader

REMOVE A NODE
Removing a node is exposed to devops for cluster management.
To remove a node it MUST first be demoted otherwise and error will be returned.
If demoded, it will be removed from the replication process and a log entry config will be appended
without this node and replicated to the cluster.
The devops can then safely destroy the node.
A node with WaitToBePromoted set to true can be safely removed or force removed.

FORCE REMOVE A NODE
Force removing a node is exposed to devops for cluster management.
This must be done with cautious because:
- NO demoting node is involved
- NO check is done
The leader symply remove it from the replication process and a log entry config without this node.
A node with WaitToBePromoted set to true can be safely removed or force removed.

LEAVE ON TERMINATE A NODE
Leave on terminate a node is NOT exposed to devops for cluster management.
Before the node is going down and when LeaveOnTerminate flag is set, it will
send a leaveOnTerminate command to tell the leader that it won't be part of the cluster
anymore. This flag is usually used by read replicas.
No checks will be done when this flag is set so it must be set with cautious.

*/

const (
	// GRPCAddress defines the default address to run the grpc server
	GRPCAddress string = "127.0.0.1"

	// GRPCPort define the default port to run the grpc server
	GRPCPort uint16 = 50051
)

const (
	// electionTimeout is the defaut value used for election campaign.
	// In raft paper it's between 150 and 300 milliseconds
	// but we use more for more stability by default
	electionTimeout int = 500

	// heartbeatTimeout is the maximum time a follower will used to detect if there is a leader.
	// If no leader, a new election campaign will be started
	heartbeatTimeout int = 500

	// maxAppendEntries will hold how much append entries the leader will send to the follower at once
	maxAppendEntries uint64 = 1000

	membershipTimeoutSeconds = 10
)

// Peer holds address of the peer
type Peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr
}

// leaderMap holds the address and id of the leader
type leaderMap struct {
	// address is the address of a peer node with explicit host and port
	address string

	// id of the current peer
	id string
}

// rpcManager holds the requirements for grpc server
type rpcManager struct {
	// raftypb.RaftyServer is an interface to access to all
	// grpc funcs
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

	// ElectionTimeout is used to start new election campaign.
	// Its value will be divided by 2 in order to randomize election timing process.
	// It must be greater or equal to HeartbeatTimeout.
	// Unit is in milliseconds
	ElectionTimeout int

	// HeartbeatTimeout is used by follower without contact from the leader
	// before starting new election campaign.
	// Unit is in milliseconds
	HeartbeatTimeout int

	// TimeMultiplier is a scaling factor that will be used during election timeout
	// by electionTimeoutMin/electionTimeoutMax/leaderHeartBeatTimeout in order to avoid cluster instability
	// The default value is 1 and the maximum is 10
	TimeMultiplier uint

	// ForceStopTimeout is the timeout after which grpc server will forced to stop.
	// Default to 60s
	ForceStopTimeout time.Duration

	// MinimumClusterSize is the size minimum to have before starting prevote or election campaign
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

	// ReadReplica statuate if the current node is a read only node
	// This kind of node won't participate into any election campaign
	ReadReplica bool

	// Peers hold the list of the peers
	Peers []Peer

	// PrevoteDisabled is a boolean the allow us to directly start
	// vote election without pre vote step
	PrevoteDisabled bool

	// ShutdownOnRemove is a boolean that allow the current node to shut down
	// when Remove command as been sent during membership change request
	ShutdownOnRemove bool

	// LeaveOnTerminate is a boolean that allow the current node to completely remove itself
	// from the cluster before shutting down by sending a LeaveOnTerminate command to the leader.
	// It's usually used by read replicas nodes.
	LeaveOnTerminate bool

	// IsSingleServerCluster indicate that it's a single server cluster.
	// This is useful for development for example
	IsSingleServerCluster bool

	// BootstrapCluster indicate if the cluster must be bootstrapped before
	// proceeding to normal cluster operations
	BootstrapCluster bool
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
	// Can only be Leader, Candidate, Follower, ReadReplica, Down
	State

	// grpc listener
	listener net.Listener

	// grpcServer hold requirements for grpc server
	grpcServer *grpc.Server

	// timer is used during the election campaign or heartbeat.
	// It will be used to detect if a Follower
	// need to step up as a Candidate to then becoming a Leader
	timer *time.Ticker

	// rpcPreVoteRequestChan will be used to handle rpc call
	rpcPreVoteRequestChan chan RPCRequest

	// rpcVoteRequestChan will be used to handle rpc call
	rpcVoteRequestChan chan RPCRequest

	// rpcSendAppendEntriesRequestChan will be used to handle rpc call
	rpcAppendEntriesRequestChan chan RPCRequest

	// triggerAppendEntriesChan is the chan that will trigger append entries
	// without waiting leader hearbeat append entries
	triggerAppendEntriesChan chan triggerAppendEntries

	// rpcForwardCommandToLeaderRequestChan will be used to handle rpc client call to leader
	rpcForwardCommandToLeaderRequestChan chan RPCRequest

	// rpcAskNodeIDChan will be used to handle rpc call
	rpcAskNodeIDChan chan RPCResponse

	// rpcClientGetLeaderChan will be used to handle rpc call
	rpcClientGetLeaderChan chan RPCResponse

	// rpcMembershipChangeRequestChan will be used by the leader to handle rpc call from followers
	rpcMembershipChangeRequestChan chan RPCRequest

	// rpcMembershipChangeChan will be used to handle rpc call from the leader
	rpcMembershipChangeChan chan RPCResponse

	// rpcBootstrapClusterRequestChan will be used by the nodes when bootstrap cluster
	// is needed
	rpcBootstrapClusterRequestChan chan RPCRequest

	// leaderFound is only related to rpc call GetLeader
	leaderFound atomic.Bool

	// leaderCount is only related to rpc call GetLeader
	leaderCount atomic.Uint64

	// leader hold informations about the leader
	leader sync.Map

	// leaderLastContactDate is the last date we heard the leader
	leaderLastContactDate atomic.Value

	// leadershipTransferInProgress  is set to true we the actual leader is stepping down
	// for maintenance for example. It's used with TimeoutNow rpc request
	leadershipTransferInProgress atomic.Bool

	// leadershipTransferDisabled is only used when the actual node is forced to step down
	// as follower when it's term is lower then other node or when it loose leader lease
	leadershipTransferDisabled atomic.Bool

	// candidateForLeadershipTransfer is set to true when the actual node receive
	// a TimeoutNow rpc request from the leader and will start to initiate
	// a new election campaign
	candidateForLeadershipTransfer atomic.Bool

	// startElectionCampaign permit to start election campaign as
	// pre vote quorum as been reached
	startElectionCampaign atomic.Bool

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

	// votedFor is the node the current node voted for during the election campaign
	votedFor string

	// votedForTerm is the node the current node voted for during the election campaign
	votedForTerm atomic.Uint64

	// currentTerm is latest term seen during the voting campaign
	currentTerm atomic.Uint64

	// lastApplied is the index of the highest log entry applied to the current raft server
	lastApplied atomic.Uint64

	// lastAppliedConfigIndex is the index of the highest log entry configuration applied
	// to the current raft server
	lastAppliedConfigIndex atomic.Uint64

	// lastAppliedConfigTerm is the term of the highest log entry configuration applied
	// to the current raft server
	lastAppliedConfigTerm atomic.Uint64

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

	// waitToBePromoted is an helper when set to true will prevent current
	// node to start any election campaign
	waitToBePromoted atomic.Bool

	// decommissioning is a boolean when set to true will allow devops
	// to put this node on maintenance or to lately send a membership
	// removal command to be safely be removed from the cluster.
	// DON'T confuse it with daitToBePromoted flag
	decommissioning atomic.Bool

	// askForMembership is an helper when set to true will make the current node
	// to ask for membership in order to be part of the cluster
	askForMembership atomic.Bool

	// askForMembershipInProgress is an helper when set to true will prevent the
	// current node to constantly ask for membership when timeout is reached
	askForMembershipInProgress atomic.Bool

	// shutdownOnRemove is a boolean that allow the current node to shut down
	// when Remove or ForceRemove command as been sent in membership change request.
	// It's done like to to allow the node to cleanly reply to the requester before
	// shutting down
	shutdownOnRemove atomic.Bool

	// isBootstrapped is an helper that indicate if the current cluster
	// has already been bootstrapped. Election timeout will be reset to 30s
	// while waiting. Once boostrapped, it will switched back to initial election timeout
	isBootstrapped atomic.Bool
}

// NewRafty instantiate rafty with default configuration
// with server address and its id
func NewRafty(address net.TCPAddr, id string, options Options) (*Rafty, error) {
	r := &Rafty{
		rpcPreVoteRequestChan:                make(chan RPCRequest),
		rpcVoteRequestChan:                   make(chan RPCRequest),
		rpcAppendEntriesRequestChan:          make(chan RPCRequest),
		triggerAppendEntriesChan:             make(chan triggerAppendEntries),
		rpcForwardCommandToLeaderRequestChan: make(chan RPCRequest),
		rpcAskNodeIDChan:                     make(chan RPCResponse),
		rpcClientGetLeaderChan:               make(chan RPCResponse),
		rpcMembershipChangeRequestChan:       make(chan RPCRequest),
		rpcMembershipChangeChan:              make(chan RPCResponse),
		rpcBootstrapClusterRequestChan:       make(chan RPCRequest),
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

	if options.ForceStopTimeout == 0 {
		options.ForceStopTimeout = 60 * time.Second
	}

	if options.DataDir == "" {
		options.DataDir = filepath.Join(os.TempDir(), "rafty")
	}

	r.options = options
	r.Logger = options.Logger

	r.connectionManager = connectionManager{
		id:                           id,
		address:                      address.String(),
		logger:                       options.Logger,
		connections:                  make(map[string]*grpc.ClientConn),
		clients:                      make(map[string]raftypb.RaftyClient),
		leadershipTransferInProgress: &r.leadershipTransferInProgress,
	}

	metaFile, dataFile, err := r.newStorage()
	r.storage = storage{
		metadata: metaFile,
		data:     dataFile,
	}
	r.storage.metadata.rafty = r
	r.storage.data.rafty = r

	r.logs = r.newLogs()
	if r.options.IsSingleServerCluster {
		r.leadershipTransferDisabled.Store(true)
	}
	r.timer = time.NewTicker(r.randomElectionTimeout())
	r.quitCtx, r.stopCtx = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return r, err
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

	r.isRunning.Store(true)
	if r.getState() == Down {
		if r.options.ReadReplica {
			r.switchState(ReadReplica, stepUp, true, r.currentTerm.Load())
		} else {
			r.switchState(Follower, stepUp, true, r.currentTerm.Load())
		}
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Node successfully started")
	}

	go r.start()
	<-r.quitCtx.Done()
	r.Stop()
	return nil
}

// start run sub requirements from Start() func
func (r *Rafty) start() {
	r.wg.Add(1)
	defer r.wg.Done()

	go r.commonLoop()
	if !r.checkNodeIDs() {
		r.sendAskNodeIDRequest()
		r.startClusterWithMinimumSize()
	}

	r.stateLoop()
}

// Stop permits to stop the gRPC server by using signal
func (r *Rafty) Stop() {
	// if statement is need otherwise some tests panics
	if r.isRunning.Load() {
		r.stop()
	}
}

// stop permits to stop the gRPC server and Rafty with the provided configuration
func (r *Rafty) stop() {
	if r.options.LeaveOnTerminate && r.waitForLeader() {
		r.sendMembershipChangeLeaveOnTerminate()
	}
	r.isRunning.Store(false)
	r.stopCtx()
	// this is just a safe guard when invoking Stop function directly
	r.switchState(Down, stepDown, true, r.currentTerm.Load())
	r.release()

	timer := time.AfterFunc(r.options.ForceStopTimeout, func() {
		r.Logger.Info().
			Str("address", r.Address.String()).
			Str("id", r.id).
			Str("state", r.getState().String()).
			Msgf("Node couldn't stop gracefully in time, doing force stop")
		r.grpcServer.Stop()
	})
	defer timer.Stop()
	r.grpcServer.GracefulStop()
	r.wg.Wait()
	r.storage.close()
	r.Logger.Info().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Node successfully stopped with term %d", r.currentTerm.Load())
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
			if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !peer.ReadReplica {
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
