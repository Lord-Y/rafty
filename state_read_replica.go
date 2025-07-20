package rafty

import "time"

type readReplica struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *readReplica) init() {}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *readReplica) onTimeout() {
	if r.rafty.getState() != ReadReplica || r.rafty.askForMembershipInProgress.Load() {
		return
	}

	if r.rafty.askForMembership.Load() {
		r.askForMembership()
		return
	}

	r.rafty.sendGetLeaderRequest()
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *readReplica) release() {}

// askForMembership will contact the leader to be part of
// the cluster
func (r *readReplica) askForMembership() {
	r.rafty.askForMembershipInProgress.Store(true)
	r.rafty.sendGetLeaderRequest()
	select {
	case <-r.rafty.quitCtx.Done():
		return

	case <-time.After(5 * time.Second):
		r.rafty.sendMembershipChangeRequest(Add)
	}
}
