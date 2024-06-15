package rafty

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/Lord-Y/rafty/logger"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
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
	preVoteElectionTimeoutMin int = 75

	// preVoteElectionTimeoutMax is the maximum preVote election timeout that will be used during preRequestVotes process
	preVoteElectionTimeoutMax int = 100

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

	// isDown when set to true close the connection to the specified peer
	isDown bool
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

// Rafty is a struct representing the raft requirements
type Rafty struct {
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

	// startElectionCampain permit to start election campain as
	// pre vote quorum as been reached
	startElectionCampain bool

	// signalCtx will be used to stop all go routines
	signalCtx context.Context

	// TimeMultiplier is a scaling factor that will be used during election timeout
	// by electionTimeoutMin/electionTimeoutMax/leaderHeartBeatTimeout in order to avoid cluster instability
	// The default value is 1 and the maximum is 10
	TimeMultiplier uint
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

	// savedCurrentTerm is a copy of the currentTerm during the election campain
	savedCurrentTerm uint64
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
	}
}

func (r *Rafty) Start() {
	r.mu.Lock()
	if r.TimeMultiplier == 0 {
		r.TimeMultiplier = 1
	}
	if r.TimeMultiplier > 10 {
		r.TimeMultiplier = 10
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

	r.startElectionTimer(true)
	r.startElectionTimer(false)

	for {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		case <-r.preVoteElectionTimer.C:
			r.preVoteRequest()
		case <-r.electionTimer.C:
			if r.startElectionCampain {
				r.resetElectionTimer(false)
				r.switchState(Candidate, true)
			}

			switch r.State {
			case Follower:
				r.runAsFollower()
			case Candidate:
				r.runAsCandidate()
			case Leader:
				r.runAsLeader()
			}

		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()
		case preVote := <-r.preVoteResponseChan:
			r.handlePreVoteResponse(preVote)
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)
		case vote := <-r.voteResponseChan:
			r.handleVoteResponse(vote)
		case voteError := <-r.voteResponseErrorChan:
			r.handleVoteResponseError(voteError)

		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)

		case reader := <-r.rpcSetLeaderChanReader:
			r.handleSetLeaderReader(reader)

		case reader := <-r.rpcSendHeartbeatsChanReader:
			r.handleSendHeartbeatsReader(reader)
		}
	}
}

func (r *Rafty) runAsFollower() {
	r.switchState(Follower, false)

	now := time.Now()
	if r.LeaderLastContactDate != nil && now.Sub(*r.LeaderLastContactDate) > time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) {
		r.resetElectionTimer(true)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.leader != nil {
			r.oldLeader = r.leader
			r.leader = nil
			r.quoroms = nil
			r.Logger.Info().Msgf("Leader %s / %s has been lost for term %d", r.oldLeader.address, r.oldLeader.id, r.CurrentTerm)
		}
	}
}

func (r *Rafty) runAsCandidate() {
	r.switchState(Candidate, false)

	r.mu.Lock()
	r.CurrentTerm += 1
	r.mu.Unlock()
	savedCurrentTerm := r.CurrentTerm

	for _, peer := range r.PreCandidatePeers {
		peer := peer
		if peer.rclient != nil && !peer.isDown && r.preVoteElectionTimer != nil && r.State == Candidate {
			go func() {
				r.Logger.Trace().Msgf("Me %s / %s contact peer %s / %s with term %d", r.Address.String(), r.ID, peer.address.String(), peer.id, r.CurrentTerm)

				response, err := peer.rclient.SendVoteRequest(
					context.Background(),
					&grpcrequests.VoteRequest{
						Id:                 r.ID,
						State:              r.State.String(),
						CurrentTerm:        savedCurrentTerm,
						CurrentCommitIndex: r.CurrentCommitIndex,
						LastApplied:        r.LastApplied,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)

				if err != nil {
					r.voteResponseErrorChan <- voteResponseErrorWrapper{
						peer: peer,
						err:  err,
					}
					return
				}

				r.voteResponseChan <- voteResponseWrapper{
					peer:             peer,
					response:         response,
					savedCurrentTerm: savedCurrentTerm,
				}
			}()
		}
	}
}

func (r *Rafty) runAsLeader() {
	r.switchState(Leader, false)

	r.heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
	defer r.heartbeatTicker.Stop()

	for {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		case <-r.heartbeatTicker.C:
			r.sendHeartBeats()
		case hearbeat := <-r.heartbeatResponseChan:
			r.handleHeartBeatsResponse(hearbeat)
		case hearbeatError := <-r.heartbeatErrorChan:
			r.handleHeartBeatsResponseError(hearbeatError)
		}
	}
}

// preVoteRequest connect to peers in order to check who is the leader
// If no leader, fetch their currentTerm
// and decided if they are suitable for election campain
func (r *Rafty) preVoteRequest() {
	r.resetElectionTimer(true)
	r.PreCandidatePeers = nil

	for _, peer := range r.Peers {
		r.connectToPeer(peer.address.String())
		peer := peer
		if peer.rclient != nil && !peer.isDown {
			go func() {
				if r.preVoteElectionTimer == nil {
					return
				}
				if r.State == Leader {
					return
				}
				if r.leader != nil {
					r.stopElectionTimer(true)
					r.switchState(Follower, true)
					return
				}

				r.Logger.Trace().Msgf("Me %s / %s with state %s contact peer %s with term %d", r.Address.String(), r.ID, r.State.String(), peer.address.String(), r.CurrentTerm)

				ctx := context.Background()
				responseLeader, err := peer.rclient.GetLeader(ctx, &grpcrequests.GetLeaderRequest{
					PeerID:      r.ID,
					PeerAddress: r.Address.String(),
				})
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to get leader from peer %s", peer.address.String())
					return
				}

				peerIndex := r.getPeerSliceIndex(peer.address.String())
				r.mu.Lock()
				r.Peers[peerIndex].id = responseLeader.GetPeerID()
				r.mu.Unlock()

				if responseLeader.GetLeaderID() != "" {
					r.Logger.Info().Msgf("Peer with id %s is the leader", responseLeader.GetLeaderID())
					r.stopElectionTimer(true)
					r.switchState(Follower, true)
					return
				}

				preVoteResponse, err := peer.rclient.SendPreVoteRequest(ctx, &grpcrequests.PreVoteRequest{
					Id:          r.ID,
					State:       r.State.String(),
					CurrentTerm: r.CurrentTerm,
				},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.preVoteResponseErrorChan <- voteResponseErrorWrapper{
						peer: peer,
						err:  err,
					}
					return
				}
				r.preVoteResponseChan <- preVoteResponseWrapper{
					peer:     peer,
					response: preVoteResponse,
				}
			}()
		}
	}
}

// sendHeartBeats send heartbeats to followers
// in order to prevent re-election
func (r *Rafty) sendHeartBeats() {
	if r.State != Leader {
		return
	}

	for _, peer := range r.Peers {
		r.connectToPeer(peer.address.String())
		peer := peer
		if peer.rclient != nil && !peer.isDown {
			go func() {
				if r.State != Leader || r.heartbeatTicker == nil {
					return
				}

				r.Logger.Trace().Msgf("Me the leader %s / %s, send heartbeat to peer %s / %s for term %d", r.Address.String(), r.ID, peer.address.String(), peer.id, r.CurrentTerm)
				response, err := peer.rclient.SendHeartbeats(
					context.Background(),
					&grpcrequests.SendHeartbeatRequest{
						LeaderID:      r.ID,
						LeaderAddress: r.Address.String(),
						CurrentTerm:   r.CurrentTerm,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)

				if err != nil {
					r.heartbeatErrorChan <- heartbeatErrorWrapper{
						peer: peer,
						err:  err,
					}
					return
				}
				r.heartbeatResponseChan <- heartbeatResponseWrapper{
					peer:             peer,
					response:         response,
					savedCurrentTerm: r.CurrentTerm,
				}
			}()
		}
	}
}

func (r *Rafty) Stop() {
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
