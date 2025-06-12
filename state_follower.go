package rafty

import (
	"time"
)

type follower struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *follower) init() {}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *follower) onTimeout() {
	if r.rafty.getState() != Follower {
		return
	}

	leader := r.rafty.getLeader()
	leaderLastContactDate := r.rafty.leaderLastContactDate.Load()
	if leaderLastContactDate != nil {
		since := time.Since(leaderLastContactDate.(time.Time))
		if since > r.rafty.heartbeatTimeout() {
			r.rafty.Logger.Info().
				Str("address", r.rafty.Address.String()).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("oldLeaderAddress", leader.address).
				Str("oldLeaderId", leader.id).
				Str("leaderHeartbeat", (r.rafty.heartbeatTimeout()/2).String()).
				Msgf("Leader has been lost for term %d since %s", r.rafty.currentTerm.Load(), since)
			r.rafty.setLeader(leaderMap{})
			r.rafty.votedFor = ""
			r.rafty.startElectionCampain.Store(false)
		}
	}
	r.rafty.switchState(Candidate, stepUp, false, r.rafty.currentTerm.Load())
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *follower) release() {}
