package rafty

import (
	"math/rand"
	"time"
)

// randomElectionTimeout permit to generate a random value
// that will be used during the election campain
// when preVote set to true, a value will be generated for preVote election
func (r *Rafty) randomElectionTimeout(preVote bool) time.Duration {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	if preVote {
		return time.Duration(preVoteElectionTimeoutMin+rd.Intn(preVoteElectionTimeoutMax-preVoteElectionTimeoutMin)) * time.Millisecond * time.Duration(r.TimeMultiplier)
	}
	return time.Duration(electionTimeoutMin+rd.Intn(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond * time.Duration(r.TimeMultiplier)
}

// startElectionTimer permit during election campain to start electionTimer
func (r *Rafty) startElectionTimer(preVote, electionCampain bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if preVote {
		r.preVoteElectionTimerEnabled.Store(true)
		r.preVoteElectionTimer = time.NewTimer(r.randomElectionTimeout(true))
	}
	if electionCampain {
		r.electionTimerEnabled.Store(true)
		r.electionTimer = time.NewTimer(r.randomElectionTimeout(false))
	}
}

// resetElectionTimer permit during election campain to reset electionTimer
func (r *Rafty) resetElectionTimer(preVote, electionCampain bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if preVote {
		r.preVoteElectionTimerEnabled.Store(true)
		r.preVoteElectionTimer.Reset(r.randomElectionTimeout(true))
	}
	if electionCampain {
		r.electionTimerEnabled.Store(true)
		r.electionTimer.Reset(r.randomElectionTimeout(false))
	}
}

// stopElectionTimer permit during election campain to stop electionTimer
func (r *Rafty) stopElectionTimer(preVote, electionCampain bool) {
	if preVote {
		if r.preVoteElectionTimer != nil {
			r.preVoteElectionTimer.Stop()
			r.preVoteElectionTimerEnabled.Store(false)
		}
	}
	if electionCampain {
		if r.electionTimer != nil {
			r.electionTimer.Stop()
			r.electionTimerEnabled.Store(false)
		}
	}
}
