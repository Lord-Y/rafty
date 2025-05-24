package rafty

type readOnly struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *readOnly) init() {
	r.rafty.timer.Reset(r.rafty.randomElectionTimeout())
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *readOnly) onTimeout() {
	leader := r.rafty.getLeader()
	if leader == (leaderMap{}) || r.rafty.leaderLost.Load() {
		r.rafty.timer.Reset(r.rafty.randomElectionTimeout())
		r.rafty.sendGetLeaderRequest()
	}
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *readOnly) release() {}
