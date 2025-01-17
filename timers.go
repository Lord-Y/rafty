package rafty

import (
	"math/rand"
	"time"
)

// randomElectionTimeout permit to generate a random value
// that will be used during the election campain
// when preVote set to true, a value will be generated for preVote election
func (r *Rafty) randomElectionTimeout() time.Duration {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(electionTimeoutMin+rd.Intn(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond * time.Duration(r.TimeMultiplier)
}

// resetElectionTimer permit during election campain to reset electionTimer
func (r *Rafty) resetElectionTimer() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.electionTimerEnabled.Store(true)
	r.electionTimer.Reset(r.randomElectionTimeout())
}

// stopElectionTimer permit during election campain to stop electionTimer
func (r *Rafty) stopElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
		r.electionTimerEnabled.Store(false)
	}
}
