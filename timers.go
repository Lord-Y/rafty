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
// when preVote set to true, preVoteElectionTimer will be restarted
func (r *Rafty) startElectionTimer(preVote bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if preVote {
		r.preVoteElectionTimer = time.NewTimer(r.randomElectionTimeout(true))
		return
	}
	r.electionTimer = time.NewTimer(r.randomElectionTimeout(false))
}

// resetElectionTimer permit during election campain to reset electionTimer
// when preVote set to true, preVoteElectionTimer will be resetted
func (r *Rafty) resetElectionTimer(preVote bool) {
	if preVote {
		r.preVoteElectionTimer.Reset(r.randomElectionTimeout(true))
		return
	}
	r.electionTimer.Reset(r.randomElectionTimeout(false))
}

// stopElectionTimer permit during election campain to stop electionTimer
// when preVote set to true, preVoteElectionTimer will be stopped
func (r *Rafty) stopElectionTimer(preVote bool) {
	if preVote {
		if r.preVoteElectionTimer != nil {
			r.preVoteElectionTimer.Stop()
		}
		return
	}
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
}
