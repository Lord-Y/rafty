package rafty

import (
	"sync/atomic"
)

// The purpose of this helper is to make sure
// we don't have any race conditions when
// multiple goroutines are used by following
// https://go.dev/ref/mem method
// espacially https://pkg.go.dev/sync/atomic package

// getState permits to retrieve node state
func (r *Rafty) getState() State {
	addr := (*uint32)(&r.State)
	return State(atomic.LoadUint32(addr))
}

// getVotedFor permits to retrieve node votedFor and term value
func (r *Rafty) getVotedFor() (votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	votedFor, term = r.votedFor, r.votedForTerm.Load()
	return
}

// setVotedFor permits to set node votedFor and term value
func (r *Rafty) setVotedFor(votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = votedFor
	r.votedForTerm.Store(term)
}

// getLeader permits to retrieve current leader informations
func (r *Rafty) getLeader() leaderMap {
	xx, ok := r.leader.Load("leader")
	if ok {
		return xx.(leaderMap)
	}
	return leaderMap{}
}

// setLeader permits to set current leader informations
func (r *Rafty) setLeader(newLeader leaderMap) {
	var currentLeader leaderMap
	xx, ok := r.leader.Load("leader")
	if ok {
		currentLeader = xx.(leaderMap)
		r.leader.Store("oldLeader", currentLeader)
	}
	if newLeader == (leaderMap{}) {
		r.leader.Delete("leader")
		return
	}
	if currentLeader != newLeader {
		r.leader.Store("leader", newLeader)
	}
}

// getLeader permits to retrieve current leader informations
func (r *Rafty) getPeers() ([]peer, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := len(r.configuration.ServerMembers)
	peers := make([]peer, total)
	copy(peers, r.configuration.ServerMembers)

	return peers, total
}

// IsRunning return a boolean tell if the node is running
func (r *Rafty) IsRunning() bool {
	return r.isRunning.Load()
}
