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
	addr, id = r.Address.String(), r.ID
	r.mu.Unlock()
	return
}

// getNextIndex permits to safely retrieve node next index
func (r *Rafty) getNextIndex(peerId string) uint64 {
	r.murw.RLock()
	defer r.murw.RUnlock()
	return r.nextIndex[peerId]
}

// setNextIndex permits to safely set node next index
func (r *Rafty) setNextIndex(peerId string, v uint64) {
	r.murw.RLock()
	defer r.murw.RUnlock()
	r.nextIndex[peerId] = v
}

// setNextAndMatchIndex permits to retrieve node next index and match index
func (r *Rafty) getNextAndMatchIndex(peerId string) (uint64, uint64) {
	r.murw.RLock()
	defer r.murw.RUnlock()
	return r.nextIndex[peerId], r.matchIndex[peerId]
}

// setNextAndMatchIndex permits to set node next index and match index
func (r *Rafty) setNextAndMatchIndex(peerId string, nextIndex, matchIndex uint64) {
	r.murw.Lock()
	defer r.murw.Unlock()
	r.nextIndex[peerId] = nextIndex
	r.matchIndex[peerId] = matchIndex
}

// getCommitIndex permits to safely retrieve node last log index
func (r *Rafty) getLastLogIndex() uint64 {
	addr := (*uint64)(&r.lastLogIndex)
	return atomic.LoadUint64(addr)
}

// getNextIndex permits to safely retrieve node next index
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
func (r *Rafty) getPrecandidate() []Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.PreCandidatePeers
}

// appendPrecandidate permits to append node pre candidates
func (r *Rafty) appendPrecandidate(peer Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.PreCandidatePeers = append(r.PreCandidatePeers, peer)
}

// getMinimumClusterSize permits to safely retrieve node minimum cluster size
func (r *Rafty) getMinimumClusterSize() uint64 {
	addr := (*uint64)(&r.MinimumClusterSize)
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
	r.LeaderLastContactDate = &now
}

// getLeader permits to retrieve current leader id
func (r *Rafty) getLeader() *leaderMap {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
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
