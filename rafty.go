package rafty

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/Lord-Y/rafty/logger"
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

	// preVoteElectionTimeoutMin is the minimum preVote election timeout that will be used during preRequestVotes process
	preVoteElectionTimeoutMin int = 70

	// preVoteElectionTimeoutMax is the maximum preVote election timeout that will be used during preRequestVotes process
	preVoteElectionTimeoutMax int = 150

	// electionTimeoutMin is the minimum election timeout that will be used to elect a new leader
	electionTimeoutMin int = 150

	// electionTimeoutMax is the maximum election timeout that will be used to elect a new leader
	electionTimeoutMax int = 300

	// leaderHeartBeatTimeout is the maximum time a leader will send heartbeats
	// after this amount of time, the leader will be considered down
	// and a new leader election campain will be started
	leaderHeartBeatTimeout int = 75

	// maxAppendEntries will hold how much append entries the leader will send to the follower at once
	maxAppendEntries uint64 = 10000
)

var (
	errAppendEntriesToLeader = errors.New("Cannot append entries from leader")
	errTermTooOld            = errors.New("Requester term older than mine")
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

	// client hold gprc server connection
	client *grpc.ClientConn

	// rclient hold gprc rafty client
	rclient grpcrequests.RaftyClient
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
	grpcrequests.UnimplementedGreeterServer
	grpcrequests.RaftyServer

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

	// preVoteElectionTimer is used during the preVote campain
	// but also to detect if the Follower server
	// need to step up as a Candidate server
	preVoteElectionTimer *time.Timer

	// preVoteElectionTimerEnabled is a boolean that allow us in some cases
	// to now if preVoteElectionTimer has been started or resetted.
	// preVoteElectionTimer can never be nil once initialiazed so see this variable as an helper
	preVoteElectionTimerEnabled atomic.Bool

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
	rpcPreVoteRequestChanWritter chan *grpcrequests.PreVoteResponse

	// rpcSendVoteRequestChanReader will be use to handle rpc call
	rpcSendVoteRequestChanReader chan *grpcrequests.VoteRequest

	// rpcSendVoteRequestChanWritter will be use to answer rpc call
	rpcSendVoteRequestChanWritter chan *grpcrequests.VoteResponse

	// rpcGetLeaderChanReader will be use to handle rpc call
	rpcGetLeaderChanReader chan *grpcrequests.GetLeaderRequest

	// rpcGetLeaderChanWritter will be use to answer rpc call
	rpcGetLeaderChanWritter chan *grpcrequests.GetLeaderResponse

	// rpcSetLeaderChanReader will be use to handle rpc call
	rpcSetLeaderChanReader chan *grpcrequests.SetLeaderRequest

	// rpcSetLeaderChanWritter will be use to answer rpc call
	rpcSetLeaderChanWritter chan *grpcrequests.SetLeaderResponse

	// rpcSendHeartbeatsChanReader will be use to handle rpc call
	rpcSendHeartbeatsChanReader chan *grpcrequests.SendHeartbeatRequest

	// rpcSendHeartbeatsChanWritter will be use to answer rpc call
	rpcSendHeartbeatsChanWritter chan *grpcrequests.SendHeartbeatResponse

	// rpcSendAppendEntriesRequestChanReader will be use to handle rpc call
	rpcSendAppendEntriesRequestChanReader chan *grpcrequests.AppendEntryRequest

	// rpcSendAppendEntriesRequestChanWritter will be use to answer rpc call
	rpcSendAppendEntriesRequestChanWritter chan *grpcrequests.AppendEntryResponse

	// rpcForwardCommandToLeaderRequestChanReader will be use to handle rpc client call to leader
	rpcForwardCommandToLeaderRequestChanReader chan *grpcrequests.ForwardCommandToLeaderRequest

	// rpcForwardCommandToLeaderRequestChanWritter will be use to answer rpc client call from leader
	rpcForwardCommandToLeaderRequestChanWritter chan *grpcrequests.ForwardCommandToLeaderResponse

	// rpcClientGetLeaderChanReader will be use to handle rpc call
	rpcClientGetLeaderChanReader chan *grpcrequests.ClientGetLeaderRequest

	// rpcClientGetLeaderChanWritter will be use to answer rpc call
	rpcClientGetLeaderChanWritter chan *grpcrequests.ClientGetLeaderResponse

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

	// PreCandidatePeers hold the list of the peers
	// that will be use to elect a new leader
	// if no leader has been detected
	PreCandidatePeers []Peer

	// Peers hold the list of the peers
	Peers []Peer

	// oldLeader hold informations about the old leader
	oldLeader *leaderMap

	// leader hold informations about the leader
	leader *leaderMap

	// leaderLost is a boolean that allow the node to properly
	// restart pre election campain when leader is lost
	leaderLost atomic.Bool

	// startElectionCampain permit to start election campain as
	// pre vote quorum as been reached
	startElectionCampain atomic.Bool

	// quit will be used to stop all go routines
	quit chan struct{}

	// TimeMultiplier is a scaling factor that will be used during election timeout
	// by electionTimeoutMin/electionTimeoutMax/leaderHeartBeatTimeout in order to avoid cluster instability
	// The default value is 1 and the maximum is 10
	TimeMultiplier uint

	// MinimumClusterSize is the size minimum to have before starting prevote or election campain
	// default is 3
	// all members of the cluster will be contacted before any other tasks
	MinimumClusterSize uint64

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
	nextIndex map[string]uint64

	// matchIndex is for each server, index of the highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotically
	matchIndex map[string]uint64

	// volatileStateInitialized is an helper to initialized
	// nextIndex and matchIndex for each peers according to raft paper
	volatileStateInitialized atomic.Bool

	// log hold all logs entries
	log []*grpcrequests.LogEntry

	// MaxAppendEntries will hold how much append entries the leader will send to the follower at once
	MaxAppendEntries uint64
}

// preVoteResponseWrapper is a struct that will be used to send response to the appropriate channel
type preVoteResponseWrapper struct {
	// peer hold the peer address
	peer Peer

	// response hold the message returned by peers
	response *grpcrequests.PreVoteResponse
}

// voteResponseWrapper is a struct that will be used to send response to the appropriate channel
type voteResponseWrapper struct {
	// peer hold the peer address
	peer Peer

	// response hold the message returned by peers
	response *grpcrequests.VoteResponse

	// savedCurrentTerm is a copy of the currentTerm during the election campain
	savedCurrentTerm uint64
}

// requestvoteResponseErrorWrapper is a struct that will be used to handle errors and sent back to the appropriate channel
type voteResponseErrorWrapper struct {
	// peer hold the peer address
	peer Peer

	// err is the error itself
	err error
}

func NewRafty() *Rafty {
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()

	return &Rafty{
		Logger:                                      &logger,
		quit:                                        make(chan struct{}),
		preVoteResponseChan:                         make(chan preVoteResponseWrapper),
		preVoteResponseErrorChan:                    make(chan voteResponseErrorWrapper),
		voteResponseChan:                            make(chan voteResponseWrapper),
		voteResponseErrorChan:                       make(chan voteResponseErrorWrapper),
		triggerAppendEntriesChan:                    make(chan triggerAppendEntries),
		rpcPreVoteRequestChanReader:                 make(chan struct{}),
		rpcPreVoteRequestChanWritter:                make(chan *grpcrequests.PreVoteResponse),
		rpcSendVoteRequestChanReader:                make(chan *grpcrequests.VoteRequest),
		rpcSendVoteRequestChanWritter:               make(chan *grpcrequests.VoteResponse),
		rpcGetLeaderChanReader:                      make(chan *grpcrequests.GetLeaderRequest),
		rpcGetLeaderChanWritter:                     make(chan *grpcrequests.GetLeaderResponse),
		rpcSendHeartbeatsChanReader:                 make(chan *grpcrequests.SendHeartbeatRequest),
		rpcSendHeartbeatsChanWritter:                make(chan *grpcrequests.SendHeartbeatResponse),
		rpcSendAppendEntriesRequestChanReader:       make(chan *grpcrequests.AppendEntryRequest),
		rpcSendAppendEntriesRequestChanWritter:      make(chan *grpcrequests.AppendEntryResponse),
		rpcForwardCommandToLeaderRequestChanReader:  make(chan *grpcrequests.ForwardCommandToLeaderRequest),
		rpcForwardCommandToLeaderRequestChanWritter: make(chan *grpcrequests.ForwardCommandToLeaderResponse),

		// client rpc
		rpcClientGetLeaderChanReader:  make(chan *grpcrequests.ClientGetLeaderRequest),
		rpcClientGetLeaderChanWritter: make(chan *grpcrequests.ClientGetLeaderResponse),
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

	if r.ID == "" {
		r.ID = uuid.NewString()
	}

	if r.getState() == Down {
		r.switchState(Follower, false, r.getCurrentTerm())
		r.Logger.Info().Msgf("Me %s / %s is starting as %s", r.Address.String(), r.ID, r.State.String())
	}
	r.mu.Unlock()

	err := r.parsePeers()
	if err != nil {
		r.Logger.Fatal().Err(err).Msg("Fail to parse peer ip/port")
	}

	for _, peer := range r.Peers {
		r.connectToPeer(peer.address.String())
	}

	r.startClusterWithMinimumSize()
	r.startElectionTimer(true, true)
	go r.loopingOverNodeState()
	go r.checkLeaderLastContactDate()
}

// startClusterWithMinimumSize allow us to reach minimum cluster size
// before doing anything else
func (r *Rafty) startClusterWithMinimumSize() {
	myAddress, myId := r.getMyAddress()
	for {
		if r.MinimumClusterSize == r.clusterSizeCounter+1 {
			r.Logger.Info().Msgf("Minimum cluster size has been reached for me %s / %s, %d out of %d", myAddress, myId, r.clusterSizeCounter+1, r.MinimumClusterSize)
			break
		}

		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return

		case <-time.After(5 * time.Second):
			r.Logger.Info().Msgf("Minimum cluster size has not been reached for me %s / %s, %d out of %d", r.Address.String(), r.ID, r.clusterSizeCounter+1, r.MinimumClusterSize)
			r.SendGetLeaderRequest()

		// handle get leader requests from other nodes
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)
		}
	}
}

// SendGetLeaderRequest allow the current node
// ask to other nodes who is the actual leader
// it also permit to get id of other nodes
func (r *Rafty) SendGetLeaderRequest() {
	currentTerm := r.getCurrentTerm()
	myAddress, myId := r.getMyAddress()
	state := r.getState()

	for _, peer := range r.Peers {
		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.leader == nil {
			if !r.healthyPeer(peer) {
				return
			}
			go func() {
				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s with term %d to ask who is the leader", myAddress, myId, state.String(), peer.address.String(), currentTerm)

				response, err := peer.rclient.GetLeader(
					context.Background(),
					&grpcrequests.GetLeaderRequest{
						PeerID:      r.ID,
						PeerAddress: r.Address.String(),
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to get leader from peer %s", peer.address.String())
					return
				}

				peerIndex := r.getPeerSliceIndex(peer.address.String())
				r.Peers[peerIndex].id = response.GetPeerID()

				if response.GetLeaderID() != "" {
					r.Logger.Info().Msgf("Peer with %s / %s is the leader", response.GetLeaderAddress(), response.GetLeaderID())
					r.switchState(Follower, false, currentTerm)
					return
				}
			}()
		}
	}
}
