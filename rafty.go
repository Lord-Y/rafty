package rafty

import (
	"context"
	"net"
	"slices"
	"sync"
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
type State uint16

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
	preVoteElectionTimeoutMin int = 90

	// preVoteElectionTimeoutMax is the maximum preVote election timeout that will be used during preRequestVotes process
	preVoteElectionTimeoutMax int = 120

	// electionTimeoutMin is the minimum election timeout that will be used to elect a new leader
	electionTimeoutMin int = 150

	// electionTimeoutMax is the maximum election timeout that will be used to elect a new leader
	electionTimeoutMax int = 300

	// leaderHeartBeatTimeout is the maximum time a leader will send heartbeats
	// after this amount of time, the leader will be considered down
	// and a new leader election campain will be started
	leaderHeartBeatTimeout int = 75
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

	// heartbeatTicker is used when the current server is the Leader
	// It will be used to send heartbeats to Followers
	heartbeatTicker *time.Ticker

	// heartbeatResponseChan is the chan that will receive hearbeats reply
	heartbeatResponseChan chan heartbeatResponseWrapper

	// heartbeatErrorChan is the chan that will receive hearbeats error
	heartbeatErrorChan chan heartbeatErrorWrapper

	// preVoteElectionTimer is used during the preVote campain
	// but also to detect if the Follower server
	// need to step up as a Candidate server
	preVoteElectionTimer *time.Timer

	// electionTimer is used during the election campain
	// but also to detect if a Follower
	// need to step up as a Candidate server
	electionTimer *time.Timer

	// preVoteResponseChan is the chan that will receive pre vote reply
	preVoteResponseChan chan preVoteResponseWrapper

	// preVoteResponseErrorChan is the chan that will receive vote reply error
	preVoteResponseErrorChan chan voteResponseErrorWrapper

	// voteResponseChan is the chan that will receive vote reply
	voteResponseChan chan voteResponseWrapper

	// voteResponseErrorChan is the chan that will receive vote reply error
	voteResponseErrorChan chan voteResponseErrorWrapper

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
	rpcSendAppendEntriesRequestChanReader chan *grpcrequests.SendAppendEntryRequest

	// rpcSendAppendEntriesRequestChanWritter will be use to answer rpc call
	rpcSendAppendEntriesRequestChanWritter chan *grpcrequests.SendAppendEntryResponse

	// rpcClientGetLeaderChanReader will be use to handle rpc call
	rpcClientGetLeaderChanReader chan *grpcrequests.ClientGetLeaderRequest

	// rpcClientGetLeaderChanWritter will be use to answer rpc call
	rpcClientGetLeaderChanWritter chan *grpcrequests.ClientGetLeaderResponse

	// quoroms hold the list of the voters with their decisions
	quoroms []*quorom

	wg sync.WaitGroup

	// mu is use to ensure lock concurrency
	mu sync.Mutex

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	Status

	// PreCandidatePeers hold the list of the peers
	// that will be use to elect a new leader
	// if no leader has been detected
	PreCandidatePeers []*Peer

	// Peers hold the list of the peers
	Peers []*Peer

	// oldLeader hold informations about the old leader
	oldLeader *leaderMap

	// leader hold informations about the leader
	leader *leaderMap

	// leaderLost is a boolean that allow the node to properly
	// restart pre election campain when leader is lost
	leaderLost bool

	// startElectionCampain permit to start election campain as
	// pre vote quorum as been reached
	startElectionCampain bool

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
}

// preVoteResponseWrapper is a struct that will be used to send response to the appropriate channel
type preVoteResponseWrapper struct {
	// peer hold the peer address
	peer *Peer

	// response hold the message returned by peers
	response *grpcrequests.PreVoteResponse
}

// voteResponseWrapper is a struct that will be used to send response to the appropriate channel
type voteResponseWrapper struct {
	// peer hold the peer address
	peer *Peer

	// response hold the message returned by peers
	response *grpcrequests.VoteResponse

	// savedCurrentTerm is a copy of the currentTerm during the election campain
	savedCurrentTerm uint64
}

// requestvoteResponseErrorWrapper is a struct that will be used to handle errors and sent back to the appropriate channel
type voteResponseErrorWrapper struct {
	// peer hold the peer address
	peer *Peer

	// err is the error itself
	err error
}

// voteResponseWrapper is a struct that will be used to send response to the appropriate channel
type heartbeatResponseWrapper struct {
	// peer hold the peer address
	peer *Peer

	// response hold the message returned by peers
	response *grpcrequests.SendHeartbeatResponse

	// currentTerm is a copy of the currentTerm during the election campain
	currentTerm uint64
}

// heartbeatErrorWrapper is a struct that will be used to handle errors and sent back to the appropriate channel
type heartbeatErrorWrapper struct {
	// peer hold the peer address
	peer *Peer

	// err is the error itself
	err error
}

func NewRafty() *Rafty {
	logger := logger.NewLogger().With().Str("logProvider", "rafty").Logger()

	return &Rafty{
		Logger:                                 &logger,
		quit:                                   make(chan struct{}),
		preVoteResponseChan:                    make(chan preVoteResponseWrapper),
		preVoteResponseErrorChan:               make(chan voteResponseErrorWrapper),
		voteResponseChan:                       make(chan voteResponseWrapper),
		voteResponseErrorChan:                  make(chan voteResponseErrorWrapper),
		heartbeatResponseChan:                  make(chan heartbeatResponseWrapper),
		heartbeatErrorChan:                     make(chan heartbeatErrorWrapper),
		rpcPreVoteRequestChanReader:            make(chan struct{}),
		rpcPreVoteRequestChanWritter:           make(chan *grpcrequests.PreVoteResponse),
		rpcSendVoteRequestChanReader:           make(chan *grpcrequests.VoteRequest),
		rpcSendVoteRequestChanWritter:          make(chan *grpcrequests.VoteResponse),
		rpcGetLeaderChanReader:                 make(chan *grpcrequests.GetLeaderRequest),
		rpcGetLeaderChanWritter:                make(chan *grpcrequests.GetLeaderResponse),
		rpcSendHeartbeatsChanReader:            make(chan *grpcrequests.SendHeartbeatRequest),
		rpcSendHeartbeatsChanWritter:           make(chan *grpcrequests.SendHeartbeatResponse),
		rpcSendAppendEntriesRequestChanReader:  make(chan *grpcrequests.SendAppendEntryRequest),
		rpcSendAppendEntriesRequestChanWritter: make(chan *grpcrequests.SendAppendEntryResponse),

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

	if r.ID == "" {
		r.ID = uuid.NewString()
	}

	if r.State == Down {
		r.State = Follower
		r.Logger.Info().Msgf("Me, id %s / %s is starting as %s", r.Address.String(), r.ID, r.State.String())
	}
	r.mu.Unlock()

	err := r.parsePeers()
	if err != nil {
		r.Logger.Fatal().Err(err).Msg("Fail to parse peer ip/port")
	}

	for {
		if r.MinimumClusterSize == r.clusterSizeCounter+1 {
			r.Logger.Info().Msgf("Minimum cluster size has been reached for me %s / %s, %d out of %d", r.Address.String(), r.ID, r.clusterSizeCounter+1, r.MinimumClusterSize)
			break
		}

		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quit:
			return

		case <-time.After(5 * time.Second):
			r.Logger.Info().Msgf("Minimum cluster size has not been reached for me %s / %s, %d out of %d", r.Address.String(), r.ID, r.clusterSizeCounter+1, r.MinimumClusterSize)
			r.getLeaderRequest()
		}
	}

	// sleeping a bit to make sure all remaining getLeaderRequest calls are closed
	time.Sleep(2 * time.Second)

	r.startElectionTimer(true)
	r.startElectionTimer(false)

	for {
		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quit:
			return

		// handle get leader requests from other nodes
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)

		// when pre vote election timer time out, start a pre vote election
		case <-r.preVoteElectionTimer.C:
			r.preVoteRequest()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// when vote election timer time out, start a new election campain
		// if pre vote election succeed
		case <-r.electionTimer.C:
			if r.State == Candidate && r.startElectionCampain {
				r.resetElectionTimer(false)
				// r.switchState(Candidate, true, r.CurrentTerm)
			}

			switch r.State {
			case Follower:
				r.runAsFollower()
			case Candidate:
				r.runAsCandidate()
			case Leader:
				r.runAsLeader()
			}

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle vote response from other nodes
		// and become a leader if conditions are met
		case vote := <-r.voteResponseChan:
			r.handleVoteResponse(vote)

		// handle vote response error from other nodes
		case voteError := <-r.voteResponseErrorChan:
			r.handleVoteResponseError(voteError)

		// handle heartbeats from leader
		case reader := <-r.rpcSendHeartbeatsChanReader:
			r.handleSendHeartbeatsReader(reader)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()
		}
	}
}

func (r *Rafty) runAsFollower() {
	r.switchState(Follower, false, r.CurrentTerm)

	now := time.Now()
	// if we haven't heard the leader
	if r.LeaderLastContactDate != nil && now.Sub(*r.LeaderLastContactDate) > time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) {
		r.resetElectionTimer(true)
		r.mu.Lock()
		r.leaderLost = true
		if r.leader != nil {
			r.oldLeader = r.leader
			r.Logger.Info().Msgf("Leader %s / %s has been lost for term %d", r.oldLeader.address, r.oldLeader.id, r.CurrentTerm)
			r.leader = nil
			r.quoroms = nil
			r.votedFor = ""
			r.PreCandidatePeers = nil
		}
		r.mu.Unlock()
	}
}

func (r *Rafty) runAsCandidate() {
	if r.State != Candidate {
		return
	}

	r.mu.Lock()
	r.CurrentTerm += 1
	currentTerm := r.CurrentTerm
	state := r.State.String()
	currentCommitIndex := r.CurrentCommitIndex
	lastApplied := r.LastApplied
	r.mu.Unlock()

	r.Logger.Info().Msgf("Starting election campain with term %d", currentTerm)
	for _, peer := range r.PreCandidatePeers {
		r.mu.Lock()
		myAddress := r.Address.String()
		myId := r.ID
		peer := *peer
		r.mu.Unlock()
		if peer.id == "" {
			r.Logger.Info().Msgf("Peer %s has no id so we cannot start the election", peer.address.String())
			r.switchState(Follower, true, currentTerm)
			r.startElectionCampain = false
			r.stopElectionTimer(false)
			r.resetElectionTimer(true)
			return
		}

		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.State == Candidate {
			if !r.healthyPeer(peer) {
				return
			}
			go func() {
				r.Logger.Trace().Msgf("Me %s / %s contact peer %s / %s with term %d for election campain", myAddress, myId, peer.address.String(), peer.id, currentTerm)

				response, err := peer.rclient.SendVoteRequest(
					context.Background(),
					&grpcrequests.VoteRequest{
						Id:                 myId,
						State:              state,
						CurrentTerm:        currentTerm,
						CurrentCommitIndex: currentCommitIndex,
						LastApplied:        lastApplied,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)

				if err != nil {
					r.voteResponseErrorChan <- voteResponseErrorWrapper{
						peer: &peer,
						err:  err,
					}
					return
				}

				r.voteResponseChan <- voteResponseWrapper{
					peer:             &peer,
					response:         response,
					savedCurrentTerm: currentTerm,
				}
			}()
		}
	}
}

func (r *Rafty) runAsLeader() {
	timer := time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond
	r.heartbeatTicker = time.NewTicker(timer)
	defer r.heartbeatTicker.Stop()

	for {
		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quit:
			return

		// handle heartbeats from other leader
		case reader := <-r.rpcSendHeartbeatsChanReader:
			r.handleSendHeartbeatsReader(reader)

		// handle heartbeat request from other nodes
		case hearbeat := <-r.heartbeatResponseChan:
			r.handleHeartBeatsResponse(hearbeat)

		// handle heartbeat response error from other nodes
		case hearbeatError := <-r.heartbeatErrorChan:
			r.handleHeartBeatsResponseError(hearbeatError)

		// handle get leader requests from other nodes
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()

		default:
			// go select is non-deterministic so as per doc it will randomly
			// select a chan and we do not want the random part,
			// that why we put this new select block here in order to have a high/low priority channel handler
			// Channels above have priorities over this one
			//nolint gosimple
			select {
			// send heartbeats to other nodes when ticker time out
			case <-r.heartbeatTicker.C:
				if r.State != Leader {
					return
				}
				for _, peer := range r.Peers {
					peer := *peer
					r.sendHeartBeats(peer)
				}
			}
		}
	}
}

// getLeaderRequest allow the current node
// ask to other nodes who is the actual leader
// it also permit to get id of other nodes
func (r *Rafty) getLeaderRequest() {
	for _, peer := range r.Peers {
		r.mu.Lock()
		currentTerm := r.CurrentTerm
		myAddress := r.Address.String()
		myId := r.ID
		state := r.State
		peer := *peer
		r.mu.Unlock()
		r.connectToPeer(peer.address.String())

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
				r.mu.Lock()
				r.Peers[peerIndex].id = response.GetPeerID()
				r.mu.Unlock()

				if response.GetLeaderID() != "" {
					r.Logger.Info().Msgf("Peer with id %s is the leader", response.GetLeaderID())
					r.switchState(Follower, true, currentTerm)
					return
				}
			}()
		}
	}
}

// preVoteRequest connect to peers in order to check who is the leader
// If no leader, fetch their currentTerm
// and decided if they are suitable for election campain
func (r *Rafty) preVoteRequest() {
	if r.State != Follower {
		r.stopElectionTimer(true)
		return
	}

	r.resetElectionTimer(true)
	r.PreCandidatePeers = nil

	for _, peer := range r.Peers {
		r.mu.Lock()
		if r.State != Follower {
			r.mu.Unlock()
			return
		}
		currentTerm := r.CurrentTerm
		myAddress := r.Address.String()
		myId := r.ID
		state := r.State
		peer := *peer
		r.mu.Unlock()
		r.connectToPeer(peer.address.String())

		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) {
			if !r.healthyPeer(peer) {
				return
			}
			go func() {
				if r.leader != nil {
					r.stopElectionTimer(true)
					r.switchState(Follower, true, currentTerm)
					return
				}
				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s with term %d for pre vote request", myAddress, myId, state.String(), peer.address.String(), currentTerm)

				response, err := peer.rclient.SendPreVoteRequest(
					context.Background(),
					&grpcrequests.PreVoteRequest{
						Id:          myId,
						State:       state.String(),
						CurrentTerm: currentTerm,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.preVoteResponseErrorChan <- voteResponseErrorWrapper{
						peer: &peer,
						err:  err,
					}
					return
				}
				r.preVoteResponseChan <- preVoteResponseWrapper{
					peer:     &peer,
					response: response,
				}
			}()
		}
	}
}

// sendHeartBeats send heartbeats to followers
// in order to prevent re-election
func (r *Rafty) sendHeartBeats(peer Peer) {
	r.mu.Lock()
	if r.State != Leader {
		r.mu.Unlock()
		return
	}
	currentTerm := r.CurrentTerm
	myAddress := r.Address.String()
	myId := r.ID
	r.mu.Unlock()
	if peer.id == "" {
		return
	}
	r.connectToPeer(peer.address.String())

	if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) {
		if !r.healthyPeer(peer) {
			return
		}
		go func() {
			r.Logger.Trace().Msgf("Me the leader %s / %s, send heartbeat to peer %s / %s for term %d", myAddress, myId, peer.address.String(), peer.id, currentTerm)

			response, err := peer.rclient.SendHeartbeats(
				context.Background(),
				&grpcrequests.SendHeartbeatRequest{
					LeaderID:      myId,
					LeaderAddress: myAddress,
					CurrentTerm:   currentTerm,
				},
				grpc.WaitForReady(true),
				grpc.UseCompressor(gzip.Name),
			)

			if err != nil {
				r.heartbeatErrorChan <- heartbeatErrorWrapper{
					peer: &peer,
					err:  err,
				}
				return
			}
			r.heartbeatResponseChan <- heartbeatResponseWrapper{
				peer:        &peer,
				response:    response,
				currentTerm: currentTerm,
			}
		}()
	}
}

func (r *Rafty) StopAll() {
	if r.electionTimer != nil {
		r.stopElectionTimer(false)
	}
	if r.preVoteElectionTimer != nil {
		r.stopElectionTimer(true)
	}
	if r.heartbeatTicker != nil {
		r.heartbeatTicker.Stop()
	}
	r.disconnectToPeers()
}
