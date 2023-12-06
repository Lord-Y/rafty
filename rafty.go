package rafty

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
	"github.com/Lord-Y/rafty/logger"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// State represent the current status of the raft server.
// The state can only be Leader, Candidate, Follower, ReadOnly, Down
type State uint16

const (
	// Down state is a node that has been unreachable for a long period of time
	Down State = iota

	// ReadOnly state is a node that does not pariticipate into the voting campain
	// It's a passive node that issue not requests on his own be simply respond answer from the leader
	// This node can never become a follower
	ReadOnly

	// Follower state is a node that participate into the voting campain
	// It's a passive node that issue not requests on his own be simply respond answer from the leader
	// This node can become a candidate if all requirements are available
	Follower

	// Candidate state is a node is a node that participate into the voting campain.
	// It that can become a Leader
	Candidate

	// Candidate state is a node that was previously a Candidate
	// It received the majority of the votes including itself and get elected as the Leader.
	// It will then handle all client requests
	// Writes requests can only be done on the leader
	Leader

	// electionTimeoutMin is the minimum election timeout that will be usedd to elect a new leader
	electionTimeoutMin int = 150

	// electionTimeoutMax is the maximum election timeout that will be usedd to elect a new leader
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
		return "readonly"
	}
	return "down"
}

type quorom struct {
	// VoterID is the id of the voter
	VoterID string

	// VoteGranted tell is the result of the vote request
	VoteGranted bool
}

type Peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr

	// id of the current peer
	id string

	// client hold gprc connection
	client *grpc.ClientConn

	// rclient hold gprc rafty client
	rclient grpcrequests.RaftyClient
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
	// LeaderID is the current leader raft cluster
	LeaderID string

	// LeaderLastContactDate is the last date we heard from the leader
	LeaderLastContactDate *time.Time

	// heartbeatTicker is used when the current server is the Leader
	// will be used to send heartbeats to Followers
	heartbeatTicker *time.Ticker

	// electionTimer is used during the election campain
	// but also to detect if the Follower server
	// need to step up as a Candidate server
	electionTimer *time.Timer

	// votedFor is the node the current node voted for during the election campain
	votedFor string

	// votedForTerm is the node the current node voted for during the election campain
	votedForTerm uint64

	// voteReplyChan is the chan that will receive vote reply
	voteReplyChan chan requestVoteReplyWrapper

	// voteReplyChanErr is the chan that will receive vote reply error
	voteReplyChanErr chan requestVoteReplyErrorWrapper

	wg sync.WaitGroup

	// mu is use to ensure lock concurrency
	mu sync.Mutex

	// Logger expose zerolog so it can be override
	Logger *zerolog.Logger

	Status

	// quoroms hold the list of the voters with their decisions
	quoroms []*quorom

	// Peers hold the list of the peers
	Peers []*Peer

	// signalCtx will be used to stop all go routines
	signalCtx context.Context
}

// requestVoteReplyWrapper is a struct that will be used in go func() to send response to the appropriate channel
type requestVoteReplyWrapper struct {
	// peer hold the peer address
	peer *Peer

	// reply hold the message returned by the Follower node
	reply *grpcrequests.RequestVoteReply
}

// requestVoteReplyErrorWrapper is a struct that will be used in go func() to handle errors and sent back to the appropriate channel
type requestVoteReplyErrorWrapper struct {
	// msg is a clear message related to the error
	msg string

	// err is the error itself
	err error
}

func NewRafty() *Rafty {
	return &Rafty{
		voteReplyChan:    make(chan requestVoteReplyWrapper),
		voteReplyChanErr: make(chan requestVoteReplyErrorWrapper),
		Logger:           logger.NewLogger(),
	}
}

func (r *Rafty) loopingOverNodeState() {
	switch r.State {
	case Follower:
		r.runAsFollower()
	case Candidate:
		r.runAsCandidate()
	case Leader:
		r.runAsLeader()
	}
}

func (r *Rafty) startElectionTimer() {
	r.electionTimer = time.NewTimer(randomElectionTimeout())
}

func (r *Rafty) resetElectionTimer() {
	r.electionTimer.Reset(randomElectionTimeout())
}

func (r *Rafty) stopElectionTimer() {
	r.electionTimer.Stop()
}

func (r *Rafty) parsePeers() error {
	var cleanPeers []*Peer // means without duplicate entries
	for _, v := range r.Peers {
		var addr net.TCPAddr
		host, port, err := net.SplitHostPort(v.Address)
		if err != nil {
			if port == "" {
				addr = net.TCPAddr{
					IP:   net.ParseIP(v.Address),
					Port: int(GRPCPort),
				}
				if r.Status.Address.String() != addr.String() {
					cleanPeers = append(cleanPeers, &Peer{
						Address: addr.String(),
						address: addr,
					})
				}
			} else {
				return err
			}
		} else {
			p, err := strconv.Atoi(port)
			if err != nil {
				return err
			}
			addr = net.TCPAddr{
				IP:   net.ParseIP(host),
				Port: p,
			}
			if r.Status.Address.String() != addr.String() {
				cleanPeers = append(cleanPeers, &Peer{
					Address: addr.String(),
					address: addr,
				})
			}
		}
	}
	r.Peers = cleanPeers
	return nil
}

func (r *Rafty) Start() {
	if r.ID == "" {
		r.ID = uuid.NewString()
	}

	if r.State.String() == "down" {
		r.State = Follower
		r.Logger.Info().Msgf("me, id %s is starting as %s", r.ID, r.State.String())
	}

	err := r.parsePeers()
	if err != nil {
		r.Logger.Fatal().Err(err).Msg("fail to parse peer ip/port")
	}

	r.startElectionTimer()
	go r.loopingOverNodeState()

	for {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		case <-r.electionTimer.C:
			r.resetElectionTimer()
			r.resetDataForCandidate()
			go r.loopingOverNodeState()
		}
	}
}

func (r *Rafty) resetDataForCandidate() {
	if r.State != Leader { // fixing empty r.LeaderID
		r.quoroms = nil
		r.votedFor = ""
		r.LeaderID = ""
		r.State = Candidate
	}
}

func (r *Rafty) runAsLeader() {
	r.heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout) * time.Millisecond)
	defer r.heartbeatTicker.Stop()

	for {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		case <-r.heartbeatTicker.C:
			r.sendHeartBeats()
		}
	}
}

func (r *Rafty) runAsFollower() {
	select {
	case <-r.signalCtx.Done():
		return // stop go routine when os signal is receive or ctrl+c
	default:
		now := time.Now()
		if r.LeaderLastContactDate != nil && now.Sub(*r.LeaderLastContactDate) > time.Duration(leaderHeartBeatTimeout) {
			r.resetElectionTimer()
			r.resetDataForCandidate()
		}
	}
}

func (r *Rafty) runAsCandidate() {
	r.sendVotes()

	for {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		case <-r.electionTimer.C:
			r.resetElectionTimer()
			r.resetDataForCandidate()
		case vote := <-r.voteReplyChan:
			r.treatVote(vote)
		case voteErr := <-r.voteReplyChanErr:
			r.treatVoteError(voteErr)
		}
	}
}

// connectToPeers permits to connect to all grpc servers
func (r *Rafty) connectToPeers() {
	// don't put r.mu.Lock() / defer r.mu.Unlock()
	// because everything will be stucked
	for _, peer := range r.Peers {
		conn, err := grpc.Dial(
			peer.address.String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// grpc.WithKeepaliveParams(kacp),
			// grpc.WithBlock(),
		)

		// ctx := context.Background()
		// conn, err := grpc.DialContext(
		// 	ctx,
		// 	peer.address.String(),
		// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
		// 	grpc.WithKeepaliveParams(kacp),
		// 	grpc.FailOnNonTempDialError(true),
		// 	grpc.WithInitialWindowSize(windowSize),
		// 	grpc.WithInitialConnWindowSize(windowSize),
		// )
		if err != nil {
			r.Logger.Err(err).Msgf("fail to connect to grpc peer server %s", peer.address.String())
			return
		}

		peer.client = conn
		peer.rclient = grpcrequests.NewRaftyClient(conn)
	}
}

// disconnectToPeers permits to disconnect to all grpc servers
func (r *Rafty) disconnectToPeers() {
	for _, peer := range r.Peers {
		peer.client.Close()
		if peer.rclient != nil {
			peer.rclient = nil
		}
	}
}

// sendVotes will send a request vote to every peers
func (r *Rafty) sendVotes() {
	// if I'm still a candidate
	// this if statement is needed in case the state changed
	// during the loop
	if r.State == Candidate {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.CurrentTerm += 1
		r.Logger.Info().Msgf("stepping up as candidate for term %d", r.CurrentTerm)
		r.connectToPeers()

		for _, peer := range r.Peers {
			// if I'm still a candidate
			// this if statement is needed in case the state changed
			// during the loop
			if r.State == Candidate {
				peer := peer
				go func() {
					r.Logger.Info().Msgf("me %s contacting peer %s", r.Address.String(), peer.address.String())

					var (
						reply    requestVoteReplyWrapper
						replyErr requestVoteReplyErrorWrapper
					)

					response, err := peer.rclient.RequestVotes(
						context.Background(),
						&grpcrequests.RequestVote{
							Id:                 r.ID,
							State:              r.State.String(),
							CurrentTerm:        r.CurrentTerm,
							CurrentCommitIndex: r.CurrentCommitIndex,
							LastApplied:        r.LastApplied,
						},
						grpc.WaitForReady(true),
						grpc.UseCompressor(gzip.Name),
					)
					if err != nil {
						got := status.Code(err)
						replyErr.msg = fmt.Sprintf("fail to request vote from peer %s with status code %v", peer.address.String(), got)
						replyErr.err = err
						r.voteReplyChanErr <- replyErr
					} else {
						reply.peer = peer
						reply.reply = response
						r.voteReplyChan <- reply
					}
				}()
			}
		}
	}
}

// getPeerSliceIndex will be used to retrieve
// the index of the peer by providing its address
func (r *Rafty) getPeerSliceIndex(addr string) int {
	for k := range r.Peers {
		if r.Peers[k].address.String() == addr {
			return k
		}
	}
	return 0
}

// treatVoteError will only print error received
// by the peer we are trying to contact
func (r *Rafty) treatVoteError(err requestVoteReplyErrorWrapper) {
	r.Logger.Error().Err(err.err).Msgf(err.msg)
}

// treatVote permit to treat the vote reply
// in order become a leader or stepping down as a follower
func (r *Rafty) treatVote(vote requestVoteReplyWrapper) {
	// if I'm still a candidate
	if r.State == Candidate {
		select {
		case <-r.signalCtx.Done():
			return // stop go routine when os signal is receive or ctrl+c
		default:
			r.mu.Lock()
			defer r.mu.Unlock()
			peerIndex := r.getPeerSliceIndex(vote.peer.address.String())
			r.Peers[peerIndex].id = vote.reply.GetPeerID()

			if r.CurrentTerm < vote.reply.GetCurrentTerm() {
				r.quoroms = append(r.quoroms, &quorom{
					VoterID: vote.reply.GetPeerID(),
				})

				r.CurrentTerm = vote.reply.GetCurrentTerm()
				r.resetElectionTimer()
				r.votedFor = ""
				r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
				r.State = Follower
				return
			}

			if vote.reply.GetNewLeaderDetected() {
				r.Logger.Info().Msgf("peer %s as been detected as new leader, stopping election campain", vote.reply.GetPeerID())
				r.resetElectionTimer()
				r.votedFor = ""
				r.CurrentTerm = vote.reply.GetCurrentTerm()
				r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
				r.State = Follower
				r.runAsFollower()
				return
			}

			if vote.reply.GetVoteGranted() {
				r.quoroms = append(r.quoroms, &quorom{
					VoterID:     vote.reply.GetPeerID(),
					VoteGranted: true,
				})
			} else {
				r.quoroms = append(r.quoroms, &quorom{
					VoterID: vote.reply.GetPeerID(),
				})

				if vote.reply.GetAlreadyVoted() {
					r.Logger.Info().Msgf("peer %s already voted for someone", vote.reply.GetPeerID())
				}
			}

			if vote.reply.RequesterStepDown {
				r.resetElectionTimer()
				r.votedFor = ""
				r.CurrentTerm = vote.reply.GetCurrentTerm()
				r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
				r.State = Follower
				r.runAsFollower()
				return
			}

			if len(r.quoroms) >= 2 {
				votes := 1 // voting for myself
				for _, q := range r.quoroms {
					if q.VoteGranted {
						votes += 1
					}
				}

				majority := votes * 100 / len(r.quoroms)
				r.Logger.Info().Msgf("majority %d quorum %d", majority, len(r.quoroms))
				switch {
				case majority > 50:
					r.resetElectionTimer()
					r.LeaderID = r.ID
					r.Logger.Info().Msgf("stepping up as new leader for term %d", r.CurrentTerm)
					r.State = Leader
					r.runAsLeader()
				case majority == 50:
					r.resetElectionTimer()
					r.Logger.Info().Msgf("we have a split vote for term %d", r.CurrentTerm)
					r.resetDataForCandidate()
				}
				return
			}
			log.Warn().Msgf("a minimum of 3 servers are needed to perform the election during term %d", r.CurrentTerm)
		}
	}
}

// randomElectionTimeout permit to generate
// a random value that will be used during
// the election campain
func randomElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(electionTimeoutMin+r.Intn(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond
}

// sendHeartBeats send heartbeats to followers
// in order to prevent re-election
func (r *Rafty) sendHeartBeats() {
	select {
	case <-r.signalCtx.Done():
		return // stop go routine when os signal is receive or ctrl+c
	default:
		r.connectToPeers()
		for _, peer := range r.Peers {
			// if I'm still the Leader
			// this if statement is needed in case the state changed
			// during the loop
			if r.State == Leader {
				peer := peer
				go func() {
					// force checking if we are still the leader in the for loop
					// to prevent behaviour or
					// if heartbeat is stopped
					if r.State != Leader || r.heartbeatTicker == nil {
						return
					}

					// prevent empty peer id behaviour
					if peer.id == "" {
						r.heartbeatTicker.Stop()
						r.LeaderID = ""
						r.resetElectionTimer()
						r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
						r.State = Follower
						r.runAsFollower()
						return
					}

					r.Logger.Info().Msgf("me the leader %s, send heartbeat to peer %s for term %d", r.LeaderID, peer.id, r.CurrentTerm)
					response, err := peer.rclient.SendHeartbeats(
						context.Background(),
						&grpcrequests.SendHeartbeatRequest{
							LeaderID:    r.ID,
							CurrentTerm: r.CurrentTerm,
						},
						grpc.WaitForReady(true),
						grpc.UseCompressor(gzip.Name),
					)

					if err != nil {
						got := status.Code(err)
						r.Logger.Err(err).Msgf("fail to request append entries to peer %s with status code %v", peer.address.String(), got)
						return
					}

					// DO NOT PUT: if response.GetMultipleLeaders() || r.CurrentTerm < response.GetCurrentTerm() {
					if response.GetMultipleLeaders() {
						r.heartbeatTicker.Stop()
						r.CurrentTerm = response.GetCurrentTerm()
						r.votedFor = ""
						r.LeaderID = ""
						r.resetElectionTimer()
						r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
						r.State = Follower
						r.runAsFollower()
					}

					// DO NOT PUT: if response.GetMultipleLeaders() || r.CurrentTerm < response.GetCurrentTerm() {
					if r.CurrentTerm < response.GetCurrentTerm() {
						r.heartbeatTicker.Stop()
						r.CurrentTerm = response.GetCurrentTerm()
						r.votedFor = ""
						r.LeaderID = ""
						r.resetElectionTimer()
						r.Logger.Info().Msgf("stepping down as follower for term %d", r.CurrentTerm)
						r.State = Follower
						r.runAsFollower()
					}
				}()
			}
		}
	}
}

func (r *Rafty) Stop() {
	if r.electionTimer != nil {
		r.stopElectionTimer()
	}
	r.disconnectToPeers()
}
