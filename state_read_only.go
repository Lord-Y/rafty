package rafty

import "time"

type readOnly struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *readOnly) init() {}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *readOnly) onTimeout() {
	if r.rafty.getState() != ReadOnly || r.rafty.askForMembershipInProgress.Load() {
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
func (r *readOnly) release() {}

// askForMembership will contact the leader to be part of
// the cluster
func (r *readOnly) askForMembership() {
	r.rafty.askForMembershipInProgress.Store(true)
	r.rafty.sendGetLeaderRequest()
	select {
	case <-r.rafty.quitCtx.Done():
		return

	case <-time.After(5 * time.Second):
		r.rafty.sendMembershipChangeRequest(Add)
	}
}
