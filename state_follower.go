package rafty

import (
	"fmt"
	"time"
)

// follower hold all requirements by a node in follower state
type follower struct {
	// rafty holds rafty config
	rafty *Rafty
}

// init initialize all requirements needed by
// the current node type
func (r *follower) init() {
	r.rafty.leadershipTransferDisabled.Store(false)
	if r.rafty.options.IsSingleServerCluster {
		r.rafty.switchState(Candidate, stepUp, true, r.rafty.currentTerm.Load())
		return
	}
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *follower) onTimeout() {
	if r.rafty.getState() != Follower || r.rafty.askForMembershipInProgress.Load() || r.rafty.decommissioning.Load() {
		return
	}

	if r.rafty.askForMembership.Load() {
		r.askForMembership()
		return
	}

	if r.rafty.options.BootstrapCluster && !r.rafty.isBootstrapped.Load() {
		r.rafty.timer.Reset(30 * time.Second)
		r.rafty.Logger.Warn().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Msgf("Waiting for the cluster to be bootstrapped")
		r.rafty.sendGetLeaderRequest()
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
				Str("candidateForLeadershipTransfer", fmt.Sprintf("%t", r.rafty.candidateForLeadershipTransfer.Load())).
				Msgf("Leader has been lost for term %d since %s", r.rafty.currentTerm.Load(), since)
			r.rafty.setLeader(leaderMap{})
			r.rafty.votedFor = ""
			r.rafty.startElectionCampaign.Store(false)
		}
	}
	r.rafty.switchState(Candidate, stepUp, true, r.rafty.currentTerm.Load())
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *follower) release() {}

// askForMembership will contact the leader to be part of
// the cluster
func (r *follower) askForMembership() {
	r.rafty.askForMembershipInProgress.Store(true)
	r.rafty.sendGetLeaderRequest()
	select {
	case <-r.rafty.quitCtx.Done():
		return

	case <-time.After(5 * time.Second):
		r.rafty.sendMembershipChangeRequest(Add)
	}
}
