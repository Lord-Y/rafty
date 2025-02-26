package rafty

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/logger"
	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
)

const (
	// GRPCAddress defines the default address to run the grpc server
	GRPCAddress string = "127.0.0.1"

	// GRPCPort define the default port to run the grpc server
	GRPCPort uint16 = 50051
)

// State represent the current status of the raft server.
// The state can only be Leader, Candidate, Follower, ReadOnly, Down
type State uint32

const (
	// Down state is a node that has been unreachable for a long period of time
	Down State = iota

	// ReadOnly state is a node that does not pariticipate into the voting campain
	// It's a passive node that issue no requests on his own but simply respond from the leader
	// This node can never become a follower
	ReadOnly

	// Follower state is a node that participate into the voting campain
	// It's a passive node that issue no requests on his own but simply respond from the leader
	// This node can become a Precandidate if all requirements are available
	Follower

	// Candidate state is a node that participate into the voting campain.
	// It can become a Leader
	Candidate

	// Leader state is a node that was previously a Candidate
	// It received the majority of the votes including itself and get elected as the Leader.
	// It will then handle all client requests
	// Writes requests can only be done on the leader
	Leader

	// electionTimeoutMin is the minimum election timeout that will be used to elect a new leader
	electionTimeoutMin int = 150

	// electionTimeoutMax is the maximum election timeout that will be used to elect a new leader
	electionTimeoutMax int = 300

	// leaderHeartBeatTimeout is the maximum time a leader will send heartbeats
	// after this amount of time, the leader will be considered lost
	// and a new leader election campain will be started
	leaderHeartBeatTimeout int = 75

	// maxAppendEntries will hold how much append entries the leader will send to the follower at once
	maxAppendEntries uint64 = 10000
)

var (
	errAppendEntriesToLeader = errors.New("Cannot append entries from leader")
	errTermTooOld            = errors.New("Requester term older than mine")
	errNoLeader              = errors.New("NoLeader")
	errCommandNotFound       = errors.New("CommandNotFound")
)

// String return a human readable state of the raft server
func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	case ReadOnly:
		return "readOnly"
	}
	return "down"
}

type quorom struct {
	// VoterID is the id of the voter
	VoterID string

	// VoteGranted tell if the vote has been granted or not
	VoteGranted bool
}

type Peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr

	// id of the current peer
	id string
}

type leaderMap struct {
	// address is the address of a peer node with explicit host and port
	address string

	// id of the current peer
	id string
}

// Status of the raft server
type Status struct {
	// ID of the current raft server
	ID string

	// Address is the current address of the raft server
	Address net.TCPAddr

	// State of the current raft server
	// Can only be Leader, Candidate, Follower, ReadOnly, Down
	State

	// CurrentTerm is latest term seen during the voting campain
	CurrentTerm uint64

	// CurrentCommitIndex is the index of the highest log entry know to be commited
	CurrentCommitIndex uint64

	// LastApplied is the index of the highest log entry applied to the current raft server
	LastApplied uint64
}

type rpcManager struct {
	raftypb.RaftyServer

	rafty *Rafty
}

// Rafty is a struct representing the raft requirements
type Rafty struct {
	// grpc listener
	listener net.Listener

	// grpcServer hold requirements for grpc server
	grpcServer *grpc.Server

	// LeaderLastContactDate is the last date we heard from the leader
	LeaderLastContactDate *time.Time

	// electionTimer is used during the election campain
	// but also to detect if a Follower
	// need to step up as a Candidate server
	electionTimer *time.Timer

	// electionTimerEnabled is a boolean that allow us in some cases
	// to now if electionTimer has been started or resetted.
	// electionTimer can never be nil once initialiazed so see this variable as an helper
	electionTimerEnabled atomic.Bool

	// preVoteResponseChan is the chan that will receive pre vote reply
	preVoteResponseChan chan preVoteResponseWrapper

	// preVoteResponseErrorChan is the chan that will receive vote reply error
	preVoteResponseErrorChan chan voteResponseErrorWrapper

	// voteResponseChan is the chan that will receive vote reply
	voteResponseChan chan voteResponseWrapper

	// voteResponseErrorChan is the chan that will receive vote reply error
	voteResponseErrorChan chan voteResponseErrorWrapper

	// triggerAppendEntriesChan is the chan that will trigger append entries
	// without waiting leader hearbeat append entries
	triggerAppendEntriesChan chan triggerAppendEntries

	// rpcPreVoteRequestChanReader will be use to handle rpc call
	rpcPreVoteRequestChanReader chan struct{}

	// rpcPreVoteRequestChanWritter will be use to answer rpc call
	rpcPreVoteRequestChanWritter chan *raftypb.PreVoteResponse

	// rpcSendVoteRequestChanReader will be use to handle rpc call
	rpcSendVoteRequestChanReader chan *raftypb.VoteRequest

	// rpcSendVoteRequestChanWritter will be use to answer rpc call
	rpcSendVoteRequestChanWritter chan *raftypb.VoteResponse

	// rpcGetLeaderChanReader will be use to handle rpc call
	rpcGetLeaderChanReader chan *raftypb.GetLeaderRequest

	// rpcGetLeaderChanWritter will be use to answer rpc call
	rpcGetLeaderChanWritter chan *raftypb.GetLeaderResponse

	// rpcSendAppendEntriesRequestChanReader will be use to handle rpc call
	rpcSendAppendEntriesRequestChanReader chan *raftypb.AppendEntryRequest

	// rpcSendAppendEntriesRequestChanWritter will be use to answer rpc call
	rpcSendAppendEntriesRequestChanWritter chan *raftypb.AppendEntryResponse

	// rpcForwardCommandToLeaderRequestChanReader will be use to handle rpc client call to leader
	rpcForwardCommandToLeaderRequestChanReader chan *raftypb.ForwardCommandToLeaderRequest

	// rpcForwardCommandToLeaderRequestChanWritter will be use to answer rpc client call from leader
	rpcForwardCommandToLeaderRequestChanWritter chan *raftypb.ForwardCommandToLeaderResponse

	// rpcClientGetLeaderChanReader will be use to handle rpc call
	rpcClientGetLeaderChanReader chan *raftypb.ClientGetLeaderRequest

	// rpcClientGetLeaderChanWritter will be use to answer rpc call
	rpcClientGetLeaderChanWritter chan *raftypb.ClientGetLeaderResponse

	// quoroms hold the list of the voters with their decisions
	quoroms []quorom

	wg sync.WaitGroup

	// mu is use to ensure lock concurrency
	mu sync.Mutex

	// murw will be mostly use with map to avoid data races
	murw sync.RWMutex

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	Status

	// Peers hold the list of the peers
	Peers []Peer

	// leader hold informations about the leader
	leader sync.Map

	// leaderLost is a boolean that allow the node to properly
	// restart pre election campain when leader is lost
	leaderLost atomic.Bool

	// startElectionCampain permit to start election campain as
	// pre vote quorum as been reached
	startElectionCampain atomic.Bool

	// quitCtx will be used to stop all go routines
	quitCtx context.Context

	// TimeMultiplier is a scaling factor that will be used during election timeout
	// by electionTimeoutMin/electionTimeoutMax/leaderHeartBeatTimeout in order to avoid cluster instability
	// The default value is 1 and the maximum is 10
	TimeMultiplier uint

	// MinimumClusterSize is the size minimum to have before starting prevote or election campain
	// default is 3
	// all members of the cluster will be contacted before any other tasks
	MinimumClusterSize uint64

	// minimumClusterSizeReach is an atomic bool flag to set
	// and start follower requirements
	minimumClusterSizeReach atomic.Bool

	// clusterSizeCounter is used to check how many nodes has been reached
	// before acknoledging the start prevote election
	clusterSizeCounter uint64

	// votedFor is the node the current node voted for during the election campain
	votedFor string

	// votedForTerm is the node the current node voted for during the election campain
	votedForTerm uint64

	// commitIndex is the highest log entry known to be committed
	// initialized to 0, increases monotically
	commitIndex uint64

	// lastLogIndex is the highest log entry applied to state machine
	// initialized to 0, increases monotically
	lastLogIndex uint64

	// nextIndex is for each server, index of the next log entry
	// to send to that server
	// initialized to leader last log index + 1
	nextIndex sync.Map

	// matchIndex is for each server, index of the highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotically
	matchIndex sync.Map

	// volatileStateInitialized is an helper to initialized
	// nextIndex and matchIndex for each peers according to raft paper
	volatileStateInitialized atomic.Bool

	// log hold all logs entries
	log []*raftypb.LogEntry

	// MaxAppendEntries will hold how much append entries the leader will send to the follower at once
	MaxAppendEntries uint64

	// DataDir is the default data directory that will be used to store all data on the disk
	// Defaults to os.TempDir()/rafty ex: /tmp/rafty
	DataDir string

	// PersistDataOnDisk is a boolean that allow us to persist data on disk
	PersistDataOnDisk bool

	// metadataFileDescriptor is the file descriptor that allow us to manage
	// metadata content
	metadataFileDescriptor *os.File

	// dataFileDescriptor is the file descriptor that allow us to manage
	// data content
	dataFileDescriptor *os.File

	// ReadOnlyNode allow to statuate if the current node is a read only node
	// This kind of node won't participate into any election campain
	ReadOnlyNode bool

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
}

// preVoteResponseWrapper is a struct that will be used to send response to the appropriate channel
type preVoteResponseWrapper struct {
	// peer hold the peer address
	peer peer

	// response hold the message returned by peers
	response *raftypb.PreVoteResponse
}

// voteResponseWrapper is a struct that will be used to send response to the appropriate channel
type voteResponseWrapper struct {
	// peer hold the peer address
	peer peer

	// response hold the message returned by peers
	response *raftypb.VoteResponse

	// savedCurrentTerm is a copy of the currentTerm during the election campain
	savedCurrentTerm uint64
}

// requestvoteResponseErrorWrapper is a struct that will be used to handle errors and sent back to the appropriate channel
type voteResponseErrorWrapper struct {
	// peer hold the peer address
	peer peer

	// err is the error itself
	err error
}

// logSource is only use during unit testing running in parallel in order to
// better debug logs
var logSource = ""

func NewRafty() *Rafty {
	var zlogger zerolog.Logger
	if logSource == "" {
		zlogger = logger.NewLogger().With().Str("logProvider", "rafty").Logger()
	} else {
		zlogger = logger.NewLogger().With().Str("logProvider", "rafty").Str("logSource", logSource).Logger()
	}

	return &Rafty{
		Logger:                                      &zlogger,
		preVoteResponseChan:                         make(chan preVoteResponseWrapper),
		preVoteResponseErrorChan:                    make(chan voteResponseErrorWrapper),
		voteResponseChan:                            make(chan voteResponseWrapper),
		voteResponseErrorChan:                       make(chan voteResponseErrorWrapper),
		triggerAppendEntriesChan:                    make(chan triggerAppendEntries),
		rpcPreVoteRequestChanReader:                 make(chan struct{}),
		rpcPreVoteRequestChanWritter:                make(chan *raftypb.PreVoteResponse),
		rpcSendVoteRequestChanReader:                make(chan *raftypb.VoteRequest),
		rpcSendVoteRequestChanWritter:               make(chan *raftypb.VoteResponse),
		rpcSendAppendEntriesRequestChanReader:       make(chan *raftypb.AppendEntryRequest),
		rpcSendAppendEntriesRequestChanWritter:      make(chan *raftypb.AppendEntryResponse),
		rpcForwardCommandToLeaderRequestChanReader:  make(chan *raftypb.ForwardCommandToLeaderRequest),
		rpcForwardCommandToLeaderRequestChanWritter: make(chan *raftypb.ForwardCommandToLeaderResponse),
		rpcGetLeaderChanReader:                      make(chan *raftypb.GetLeaderRequest),
		rpcGetLeaderChanWritter:                     make(chan *raftypb.GetLeaderResponse),
		rpcClientGetLeaderChanReader:                make(chan *raftypb.ClientGetLeaderRequest),
		rpcClientGetLeaderChanWritter:               make(chan *raftypb.ClientGetLeaderResponse),
	}
}

func (r *Rafty) start() {
	r.mu.Lock()
	if r.TimeMultiplier == 0 {
		r.TimeMultiplier = 1
	}
	if r.TimeMultiplier > 10 {
		r.TimeMultiplier = 10
	}

	if r.MinimumClusterSize == 0 {
		r.MinimumClusterSize = 3
	}

	if r.MaxAppendEntries == 0 {
		r.MaxAppendEntries = maxAppendEntries
	}

	if r.DataDir == "" {
		datadir := filepath.Join(os.TempDir(), "rafty")
		r.DataDir = datadir
		r.Logger.Debug().Msgf("default data dir is %s", datadir)
	}
	r.mu.Unlock()

	r.restoreMetadata()
	r.restoreData()

	if r.ID == "" {
		r.ID = uuid.NewString()
		if err := r.persistMetadata(); err != nil {
			r.Logger.Fatal().Err(err).Msgf("Fail to persist metadata")
		}
	}

	if r.getState() == Down {
		if r.ReadOnlyNode {
			r.switchState(ReadOnly, false, r.getCurrentTerm())
		} else {
			r.switchState(Follower, false, r.getCurrentTerm())
		}
		r.Logger.Info().Msgf("Me %s / %s is starting as %s", r.Address.String(), r.ID, r.State.String())
	}

	err := r.parsePeers()
	if err != nil {
		r.Logger.Fatal().Err(err).Msg("Fail to parse peer ip/port")
	}

	for _, peer := range r.configuration.ServerMembers {
		r.connectToPeer(peer.address.String())
	}

	r.startClusterWithMinimumSize()
	r.sendGetLeaderRequest()
	go r.loopingOverNodeState()
}

// startClusterWithMinimumSize allow us to reach minimum cluster size
// before doing anything else
func (r *Rafty) startClusterWithMinimumSize() {
	myAddress, myId := r.getMyAddress()
	for r.getState() != Down {
		time.Sleep(time.Second)
		if r.MinimumClusterSize == r.clusterSizeCounter+1 {
			r.Logger.Info().Msgf("Minimum cluster size has been reached for me %s / %s, %d out of %d", myAddress, myId, r.clusterSizeCounter+1, r.MinimumClusterSize)
			r.minimumClusterSizeReach.Store(true)
			break
		}
	}
}

// sendGetLeaderRequest allow the current node
// ask to other nodes who is the actual leader
// and prevent starting election campain
func (r *Rafty) sendGetLeaderRequest() {
	currentTerm := r.getCurrentTerm()
	myAddress, myId := r.getMyAddress()
	state := r.getState()
	var leaderFound atomic.Bool
	r.mu.Lock()
	peers := r.configuration.ServerMembers
	totalPeers := len(peers)
	r.mu.Unlock()

	for i, peer := range peers {
		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && !r.leaderLost.Load() && r.getState() != Down {
			go func() {
				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s with term %d to ask who is the leader", myAddress, myId, state.String(), peer.address.String(), currentTerm)

				response, err := peer.rclient.GetLeader(
					context.Background(),
					&raftypb.GetLeaderRequest{
						PeerID:      r.ID,
						PeerAddress: r.Address.String(),
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)

				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to get leader from peer %s", peer.address.String())
				} else {
					if response.GetLeaderID() != "" {
						r.setLeader(leaderMap{
							address: response.GetLeaderAddress(),
							id:      response.GetLeaderID(),
						})
						r.leaderLost.Store(false)
						leaderFound.Store(true)
						r.Logger.Info().Msgf("Me %s / %s with state %s reports that peer %s / %s is the leader", myAddress, myId, state, response.GetLeaderAddress(), response.GetLeaderID())
					}
				}

				if !leaderFound.Load() && i+1 == totalPeers {
					r.Logger.Info().Msgf("Me %s / %s with state %s reports that there is no leader", myAddress, myId, state)
					r.leaderLost.Store(true)
				}
			}()
		}
	}
}
