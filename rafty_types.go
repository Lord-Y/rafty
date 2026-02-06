package rafty

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

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

	// maxAppendEntries will holds how much append entries the leader will send to the follower at once
	maxAppendEntries uint64 = 1000
)

// InitialPeer holds address of the peer
type InitialPeer struct {
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

// Options holds config that will be modified by users
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

	// MaxAppendEntries will holds how much append entries the leader will send to the follower at once
	MaxAppendEntries uint64

	// DataDir is the default data directory that will be used to store all data on the disk. It's required
	DataDir string

	// IsVoter statuates if this peer is a voting member node.
	// When set to false, this node won't participate into any election campaign
	IsVoter bool

	// Peers holds the list of the peers
	InitialPeers []InitialPeer

	// PrevoteDisabled is a boolean the allows us to directly start
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

	// SnapshotInterval is the interval at which a snapshot will be taken.
	// It will be randomize with this minimum value in order to prevent
	// all nodes to take a snapshot at the same time
	// Default to 1h
	SnapshotInterval time.Duration

	// SnapshotThreshold is the threshold that must be reached between LastIncluedIndex and new logs before taking a snapshot.
	// It prevent to take snapshots to frequently.
	// Default to 64
	SnapshotThreshold uint64

	// MetricsNamespacePrefix is the namespace to use for all rafty metrics.
	// When set, the full metric name will be `<MetricsNamespacePrefix>_rafty_<metric_name>`.
	// Otherwise it will be `rafty_<metric_name>`.
	MetricsNamespacePrefix string
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
	// Can only be Leader, Candidate, Follower, Down
	State

	// grpc listener
	listener net.Listener

	// grpcServer holds requirements for grpc server
	grpcServer *grpc.Server

	// timer is used during the election campaign or heartbeat.
	// It will be used to detect if a Follower
	// need to step up as a Candidate to then becoming a Leader
	timer *time.Ticker
	// timerMu serializes timer reset/stop operations.
	timerMu sync.Mutex

	// rand is used for randomized timings.
	rand *rand.Rand
	// randMu serializes access to rand.
	randMu sync.Mutex

	// rpcPreVoteRequestChan will be used to handle rpc call
	rpcPreVoteRequestChan chan RPCRequest

	// rpcVoteRequestChan will be used to handle rpc call
	rpcVoteRequestChan chan RPCRequest

	// rpcAppendEntriesRequestChan is used by the leader to append entries
	rpcAppendEntriesRequestChan chan RPCRequest

	// rpcAppendEntriesReplicationRequestChan is used by the followers to handle rpc call
	rpcAppendEntriesReplicationRequestChan chan RPCRequest

	// rpcAskNodeIDChan will be used to handle rpc call
	rpcAskNodeIDChan chan RPCResponse

	// rpcClientGetLeaderChan will be used to handle rpc call
	rpcClientGetLeaderChan chan RPCResponse

	// rpcInstallSnapshotRequestChan will be used to install/restore snapshot
	rpcInstallSnapshotRequestChan chan RPCRequest

	// rpcBootstrapClusterRequestChan will be used by the nodes when bootstrap cluster
	// is needed
	rpcBootstrapClusterRequestChan chan RPCRequest

	// leaderFound is only related to rpc call GetLeader
	leaderFound atomic.Bool

	// leaderCount is only related to rpc call GetLeader
	leaderCount atomic.Uint64

	// leader holds informations about the leader
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

	// lastApplied is the index of the highest log entry applied to the current raft server fsm
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

	// logStore is long term storage backend only for raft logs.
	// The same long term storage for clusterStore and logStore can be used
	// BUT in different namespace or buckets to avoid collisions.
	// In logs_persistant_types, you will see different buckets for bolt storage:
	// - bucketLogsName
	// - bucketMetadataName
	// - bucketKVName
	// The same logic is applied for LogsCache and LogsInMemory.
	// User land MUST NOT modified those logs.
	// Users must use their own buckets with stateMachine when use ApplyCommand.
	logStore LogStore

	// clusterStore is long term storage backend only for the raft cluster
	// purpose like metadata and other KVs e.g.
	// The same long term storage for clusterStore and logStore can be used
	// BUT in different namespace or buckets to avoid collisions.
	// In logs_persistant_types, you will see different buckets for bolt storage:
	// - bucketLogsName
	// - bucketMetadataName
	// - bucketKVName
	// The same logic is applied for LogsCache and LogsInMemory.
	// User land MUST NOT modified those entries.
	// Users must use their own buckets with stateMachine when use ApplyCommand.
	clusterStore ClusterStore

	// snapshot holds the configuration to manage snapshots
	snapshot SnapshotStore

	// lastIncludedIndex is the index included in the last snapshot
	lastIncludedIndex atomic.Uint64

	// lastIncludedTerm is the term linked to lastIncludedIndex
	lastIncludedTerm atomic.Uint64

	// StateMachine is an interface allowing clients
	// to interact with the raft cluster
	fsm StateMachine

	// configuration holds server members found on disk
	// If empty, it will be equal to Peers list
	//
	// When a new member has been found into Peers list and not on disk
	// a cluster membership will be initiated in order to add it
	//
	// When persistant storage is not enabled and the cluster start with 3 nodes
	// if a new node is started it won't be part of the initial cluster list
	// so a cluster membership will be initiated in order to add it
	configuration Configuration

	// connectionManager holds connections for all members
	connectionManager connectionManager

	// waitToBePromoted is an helper when set to true will prevent current
	// node to start any election campaign
	waitToBePromoted atomic.Bool

	// decommissioning is a boolean when set to true will allow devops
	// to put this node on maintenance or to lately send a membership
	// removal command to be safely be removed from the cluster.
	// DON'T confuse it with daitToBePromoted flag
	decommissioning atomic.Bool

	// membershipChangeInProgress is set to true by the leader when membership change is ongoing
	membershipChangeInProgress atomic.Bool

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

	// metrics holds all prometheus metrics for rafty
	metrics *metrics
}

// Status represent the current status of the node
type Status struct {
	// State is the current state of the node
	State State

	// Leader is the current leader of the cluster, if any
	Leader leaderMap

	// currentTerm is the current term of the node
	CurrentTerm uint64

	// CommittedIndex is the last committed index of the node
	CommitIndex uint64

	// LastApplied is the last applied index of the node
	LastApplied uint64

	// LastLogIndex is the last log index of the node
	LastLogIndex uint64

	// LastLogTerm is the last log term of the node
	LastLogTerm uint64

	// LastAppliedConfigIndex is the index of the last applied configuration
	LastAppliedConfigIndex uint64

	// LastAppliedConfigTerm is the term of the last applied configuration
	LastAppliedConfigTerm uint64

	// Configuration is the current configuration of the cluster
	Configuration Configuration
}

// applyLogs is a helper struct to apply logs in the state machine
type applyLogs struct {
	// entries are the logs to apply
	entries []*raftypb.LogEntry
}

// metadata is a struct holding all requirements
// to persist node metadata
type metadata struct {
	// Id of the current raft server
	Id string `json:"id"`

	// CurrentTerm is latest term seen during the voting campaign
	CurrentTerm uint64 `json:"currentTerm"`

	// VotedFor is the node the current node voted for during the election campaign
	VotedFor string `json:"votedFor"`

	// LastApplied is the index of the highest log entry applied to the current raft server
	LastApplied uint64 `json:"lastApplied"`

	// LastAppliedConfig is the index of the highest log entry configuration applied
	// to the current raft server
	LastAppliedConfigIndex uint64 `json:"lastAppliedConfigIndex"`

	// LastAppliedConfigTerm is the term of the highest log entry configuration applied
	// to the current raft server
	LastAppliedConfigTerm uint64 `json:"lastAppliedConfigTerm"`

	// Configuration holds server members
	Configuration Configuration `json:"configuration"`

	// LastIncludedIndex is the index included in the last snapshot
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`

	// lastIncludedTerm is the term linked to LastSnapshotIndex
	LastIncludedTerm uint64 `json:"lastIncludedTerm"`
}
