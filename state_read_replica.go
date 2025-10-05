package rafty

// init initialize all requirements needed by
// the current node type
func (r *readReplica) init() {
	r.rafty.metrics.setNodeStateGauge(ReadReplica)
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *readReplica) onTimeout() {
	if r.rafty.getState() != ReadReplica || r.rafty.askForMembershipInProgress.Load() {
		return
	}

	if r.rafty.askForMembership.Load() {
		return
	}

	r.rafty.sendGetLeaderRequest()
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *readReplica) release() {}
