package rafty

import (
	"fmt"
	"slices"
	"sync"
)

// candidate hold all requirements by a node in candidate state
type candidate struct {
	// rafty holds rafty config
	rafty *Rafty

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// responsePreVoteChan is use to manage pre vote responses
	responsePreVoteChan chan RPCResponse

	// responseVoteChan is use to manage vote responses
	responseVoteChan chan RPCResponse

	// preCandidatePeers hold the list of the peers that will be used
	// during election campaign to elect a new leader
	preCandidatePeers []Peer

	// preVotes is the total number of votes used to be compare against quorum
	preVotes int

	// votes is the total number of votes used to be compare against quorum
	votes int

	// quorum is the minimum number of votes to reach before starting election
	// or becoming the new leader
	quorum int
}

// init initialize all requirements needed by
// the current node type
func (r *candidate) init() {
	if r.rafty.options.IsSingleServerCluster {
		r.isSingleServerCluster()
		return
	}
	leader := r.rafty.getLeader()
	if r.rafty.getState() == Candidate && (leader == (leaderMap{}) || r.rafty.candidateForLeadershipTransfer.Load()) {
		r.quorum = r.rafty.quorum()
		r.preVotes = 1 // vote for myself
		r.votes = 1    // vote for myself
		if r.rafty.options.PrevoteDisabled || r.rafty.candidateForLeadershipTransfer.Load() {
			r.startElection()
			return
		}
		r.preVoteRequest()
		return
	}
	r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *candidate) onTimeout() {
	if r.rafty.getState() != Candidate {
		return
	}

	if r.rafty.decommissioning.Load() {
		r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
		return
	}

	defer func() {
		_ = r.rafty.candidateForLeadershipTransfer.CompareAndSwap(true, false)
	}()

	r.preVotes = 1 // vote for myself
	r.votes = 1    // vote for myself
	if r.rafty.options.PrevoteDisabled || r.rafty.candidateForLeadershipTransfer.Load() {
		r.startElection()
		return
	}
	r.preVoteRequest()
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *candidate) release() {
	r.preVotes = 0
	r.votes = 0
	r.rafty.startElectionCampaign.Store(false)
	r.responsePreVoteChan = nil
	r.responseVoteChan = nil
}

// preVoteRequest connect to peers in order to check who is the leader
// If no leader, fetch their currentTerm
// and decided if they are suitable for election campaign
func (r *candidate) preVoteRequest() {
	currentTerm := r.rafty.currentTerm.Load() + 1
	r.rafty.switchState(Candidate, stepUp, true, currentTerm)
	r.preCandidatePeers = nil
	peers, _ := r.rafty.getPeers()

	r.responsePreVoteChan = make(chan RPCResponse, len(peers))
	request := RPCRequest{
		RPCType: PreVoteRequest,
		Request: RPCPreVoteRequest{
			Id:          r.rafty.id,
			CurrentTerm: currentTerm,
		},
		Timeout:      r.rafty.randomRPCTimeout(false),
		ResponseChan: r.responsePreVoteChan,
	}

	r.rafty.timer.Reset(r.rafty.randomElectionTimeout())

	for _, peer := range peers {
		go func() {
			client := r.rafty.connectionManager.getClient(peer.address.String())
			if client != nil && r.rafty.getState() == Candidate && !peer.ReadReplica && !peer.Decommissioning {
				r.rafty.sendRPC(request, client, peer)
			}
		}()
	}
}

// handlePreVoteResponse will treat pre vote response and select nodes
// that are suitable to start the election campaign
func (r *candidate) handlePreVoteResponse(resp RPCResponse) {
	if r.rafty.getState() != Candidate || r.rafty.candidateForLeadershipTransfer.Load() {
		return
	}

	response := resp.Response.(RPCPreVoteResponse)
	err := resp.Error
	targetPeer := resp.TargetPeer

	if err != nil {
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", response.RequesterTerm)).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Msgf("Fail to get pre vote request")
		return
	}

	if response.CurrentTerm > response.RequesterTerm {
		r.rafty.currentTerm.Store(response.CurrentTerm)
		r.rafty.switchState(Follower, stepDown, false, response.CurrentTerm)
		if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}
		return
	}

	if response.Granted {
		r.mu.Lock()
		if index := slices.IndexFunc(r.preCandidatePeers, func(peer Peer) bool {
			return peer.ID == targetPeer.ID
		}); index == -1 {
			r.preCandidatePeers = append(r.preCandidatePeers, targetPeer)
		}
		r.preVotes++
		r.mu.Unlock()

		if r.preVotes >= r.quorum && (!r.rafty.startElectionCampaign.Load() || !r.rafty.candidateForLeadershipTransfer.Load()) {
			r.rafty.startElectionCampaign.Store(true)
			r.startElection()
		}
		return
	}
}

// startElection permit to send vote request
// to other nodes in order to elect a leader
func (r *candidate) startElection() {
	currentTerm := r.rafty.currentTerm.Add(1)
	if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
		panic(err)
	}

	lastLogIndex := r.rafty.lastLogIndex.Load()
	lastLogTerm := r.rafty.lastLogTerm.Load()
	var peers []Peer
	if r.rafty.options.PrevoteDisabled || r.rafty.candidateForLeadershipTransfer.Load() {
		peers, _ = r.rafty.getPeers()
	} else {
		peers = r.preCandidatePeers
	}

	r.responseVoteChan = make(chan RPCResponse, len(peers))
	request := RPCRequest{
		RPCType: VoteRequest,
		Request: RPCVoteRequest{
			CandidateId:                    r.rafty.id,
			CandidateAddress:               r.rafty.Address.String(),
			CurrentTerm:                    currentTerm,
			LastLogIndex:                   lastLogIndex,
			LastLogTerm:                    lastLogTerm,
			CandidateForLeadershipTransfer: r.rafty.candidateForLeadershipTransfer.Load(),
		},
		Timeout:      r.rafty.randomRPCTimeout(false),
		ResponseChan: r.responseVoteChan,
	}

	r.rafty.timer.Reset(r.rafty.randomElectionTimeout())

	for _, peer := range peers {
		go func() {
			client := r.rafty.connectionManager.getClient(peer.address.String())
			if client != nil && r.rafty.getState() == Candidate && !peer.ReadReplica && !peer.Decommissioning {
				r.rafty.sendRPC(request, client, peer)
			}
		}()
	}
}

// handleVoteResponse will treat vote response and step up as leader
// or step down as follower
func (r *candidate) handleVoteResponse(resp RPCResponse) {
	defer r.rafty.candidateForLeadershipTransfer.Store(false)
	if r.rafty.getState() != Candidate {
		return
	}

	response := resp.Response.(RPCVoteResponse)
	err := resp.Error
	targetPeer := resp.TargetPeer

	if err != nil {
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", response.RequesterTerm)).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Msgf("Fail to send vote request to peer")
		return
	}

	if response.CurrentTerm > response.RequesterTerm {
		r.rafty.Logger.Info().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", response.RequesterTerm)).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Str("peerTerm", fmt.Sprintf("%d", response.CurrentTerm)).
			Msgf("Peer has higher term than me")

		r.rafty.currentTerm.Store(response.CurrentTerm)
		r.rafty.switchState(Follower, stepDown, false, response.CurrentTerm)
		if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}
		return
	}

	if response.Granted {
		r.votes++
	} else {
		r.rafty.Logger.Trace().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", targetPeer.address.String()).
			Str("peerId", targetPeer.ID).
			Msgf("Peer already voted for someone")
	}

	if r.votes >= r.quorum && r.rafty.getState() == Candidate {
		r.rafty.switchState(Leader, stepUp, true, response.RequesterTerm)
	}
}

// isSingleServerCluster is only used by a single server cluster.
// It will switch to Candidate state and then become the Leader
func (r *candidate) isSingleServerCluster() {
	currentTerm := r.rafty.currentTerm.Add(1)
	r.rafty.switchState(Candidate, stepUp, true, currentTerm)
	if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
		panic(err)
	}
	r.rafty.switchState(Leader, stepUp, true, currentTerm)
}
