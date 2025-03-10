package rafty

import (
	"sync/atomic"
	"time"
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

// getCurrentTerm permits to retrieve node current Term
func (r *Rafty) getCurrentTerm() uint64 {
	addr := (*uint64)(&r.CurrentTerm)
	return atomic.LoadUint64(addr)
}

// setCurrentTerm permits to set node current Term
func (r *Rafty) setCurrentTerm(v uint64) {
	addr := (*uint64)(&r.CurrentTerm)
	atomic.StoreUint64(addr, v)
}

// incrementCurrentTerm permits to safely increase node current Term
func (r *Rafty) incrementCurrentTerm() uint64 {
	addr := (*uint64)(&r.CurrentTerm)
	return atomic.AddUint64(addr, 1)
}

// getCommitIndex permits to safely retrieve node commit index
func (r *Rafty) getCommitIndex() uint64 {
	addr := (*uint64)(&r.commitIndex)
	return atomic.LoadUint64(addr)
}

func (r *Rafty) getMyAddress() (addr, id string) {
	r.mu.Lock()
	addr, id = r.Address.String(), r.id
	r.mu.Unlock()
	return
}

// getNextIndex permits to safely retrieve node next index
func (r *Rafty) getNextIndex(peerId string) (x uint64) {
	xx, ok := r.nextIndex.Load(peerId)
	if ok {
		return xx.(uint64)
	}
	return
}

// setNextIndex permits to safely set node next index
func (r *Rafty) setNextIndex(peerId string, v uint64) {
	r.nextIndex.Store(peerId, v)
}

// setNextAndMatchIndex permits to retrieve node next index and match index
func (r *Rafty) getNextAndMatchIndex(peerId string) (x uint64, y uint64) {
	xx, ok := r.nextIndex.Load(peerId)
	if ok {
		x = xx.(uint64)
	}
	yy, _ := r.matchIndex.Load(peerId)
	if ok {
		y = yy.(uint64)
	}
	return
}

// setNextAndMatchIndex permits to set node next index and match index
func (r *Rafty) setNextAndMatchIndex(peerId string, nextIndex, matchIndex uint64) {
	r.nextIndex.Store(peerId, nextIndex)
	r.matchIndex.Store(peerId, matchIndex)
}

// getCommitIndex permits to safely retrieve node last log index
func (r *Rafty) getLastLogIndex() uint64 {
	addr := (*uint64)(&r.lastLogIndex)
	return atomic.LoadUint64(addr)
}

// getLastLogTerm permits to safely retrieve node last log term
func (r *Rafty) getLastLogTerm(x uint64) uint64 {
	addr := (*uint64)(&x)
	return atomic.LoadUint64(addr)
}

// getX permits to safely retrieve node X data
func (r *Rafty) getX(x uint64) uint64 {
	addr := (*uint64)(&x)
	return atomic.LoadUint64(addr)
}

// getVotedFor permits to retrieve node votedFor and term value
func (r *Rafty) getVotedFor() (votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	votedFor, term = r.votedFor, r.votedForTerm
	return
}

// setVotedFor permits to set node votedFor and term value
func (r *Rafty) setVotedFor(votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor, r.votedForTerm = votedFor, term
}

// getPrecandidate permits to retrieve node pre candidates
func (r *Rafty) getPrecandidate() []peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.configuration.preCandidatePeers
}

// appendPrecandidate permits to append node pre candidates
func (r *Rafty) appendPrecandidate(peer peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.configuration.preCandidatePeers = append(r.configuration.preCandidatePeers, peer)
}

// getMinimumClusterSize permits to safely retrieve node minimum cluster size
func (r *Rafty) getMinimumClusterSize() uint64 {
	addr := (*uint64)(&r.options.MinimumClusterSize)
	return atomic.LoadUint64(addr)
}

// getTotalLogs permits to retrieve node total logs
func (r *Rafty) getTotalLogs() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.log)
}

// setLeaderLastContactDate permits to set node last leader contact date
func (r *Rafty) setLeaderLastContactDate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.leaderLastContactDate = &now
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

// incrementLeaderCommitIndex permits to safely increase leader current commit index
func (r *Rafty) incrementLeaderCommitIndex() uint64 {
	addr := (*uint64)(&r.CurrentCommitIndex)
	return atomic.AddUint64(addr, 1)
}

// incrementLastApplied permits to safely increase leader lastApplied
func (r *Rafty) incrementLastApplied() uint64 {
	addr := (*uint64)(&r.LastApplied)
	return atomic.AddUint64(addr, 1)
}
