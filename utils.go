package rafty

import (
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
)

func (r *Rafty) parsePeers() error {
	var uniqPeers []Peer
	for _, peer := range r.Peers {
		var addr net.TCPAddr
		host, port, err := net.SplitHostPort(peer.Address)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				return err
			}
			if port == "" {
				addr = net.TCPAddr{
					IP:   net.ParseIP(peer.Address),
					Port: int(GRPCPort),
				}
				if r.Status.Address.String() != addr.String() {
					uniqPeers = append(uniqPeers, Peer{
						Address: addr.String(),
						address: addr,
					})
				}
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
				uniqPeers = append(uniqPeers, Peer{
					Address: addr.String(),
					address: addr,
				})
			}
		}
	}
	r.mu.Lock()
	r.Peers = uniqPeers
	r.mu.Unlock()
	return nil
}

// getPeerSliceIndex will be used to retrieve
// the index of the peer by providing its address
func (r *Rafty) getPeerSliceIndex(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.address.String() == addr
	})
	if index != -1 {
		return index
	}
	return -1
}

// checkIfPeerInSliceIndex will be used to check
// if peer is present in the peer list
func (r *Rafty) checkIfPeerInSliceIndex(preVote bool, addr string) bool {
	if preVote {
		r.murw.Lock()
		index := slices.IndexFunc(r.PreCandidatePeers, func(p Peer) bool {
			return p.address.String() == addr
		})
		r.murw.Unlock()
		return index != -1
	}

	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.address.String() == addr
	})
	return index != -1
}

func (r *Rafty) loopingOverNodeState() {
	for r.getState() != Down {
		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quitCtx.Done():
			r.switchState(Down, true, r.getCurrentTerm())
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runAsFollower()
		case Candidate:
			r.runAsCandidate()
		case Leader:
			r.runAsLeader()
		}
	}
}

// switchState permits to switch to the mentionned state
// and print a nice message if needed
func (r *Rafty) switchState(state State, niceMessage bool, currentTerm uint64) {
	if r.getState() == state {
		return
	}
	addr := (*uint32)(&r.State)
	atomic.StoreUint32(addr, uint32(state))

	if state == Follower {
		r.volatileStateInitialized.Store(false)
	}

	if state == Down {
		r.mu.Lock()
		r.leader = nil
		r.mu.Unlock()
	}

	if niceMessage {
		myAddress, myId := r.getMyAddress()
		switch state {
		case Follower:
			r.Logger.Info().Msgf("Me %s / %s stepping down as %s for term %d", myAddress, myId, state, currentTerm)
		case Candidate:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", myAddress, myId, state, currentTerm)
		case Leader:
			r.stopElectionTimer()
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", myAddress, myId, state, currentTerm)
		case Down:
			r.Logger.Info().Msgf("Me %s / %s is shutting down with term %d", myAddress, myId, currentTerm)
		}
	}
}

// logState permits to log mentionned state
// with a nice message if needed
func (r *Rafty) logState(state State, niceMessage bool, currentTerm uint64) {
	if r.getState() == state {
		return
	}

	if niceMessage {
		switch state {
		case Follower:
			r.Logger.Info().Msgf("Me %s / %s stepping down as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		case Candidate:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		case Leader:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		}
	}
}

// saveLeaderInformations permits to copy leader informations to oldLeader
// for later use and then set it to nil
func (r *Rafty) saveLeaderInformations(newLeader leaderMap) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.State == Leader {
		if r.oldLeader == nil {
			leader := leaderMap{
				id:      r.ID,
				address: r.Address.String(),
			}
			r.oldLeader = &leader
		}
		return
	}
	if r.leader != nil && *r.leader == newLeader {
		return
	}
	r.oldLeader = r.leader
	r.leader = &newLeader
}

// min return the minimum value based on provided values
func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// max return the maximum value based on provided values
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// getPeerClient will be used to retrieve
// the index of the peer and return its related struct
func (r *Rafty) getPeerClient(peerId string) Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.id == peerId
	})
	if index != -1 {
		return r.Peers[index]
	}
	return Peer{}
}
