package rafty

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
)

type leader struct {
	// rafty holds rafty config
	rafty *Rafty

	wg sync.WaitGroup

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// leaseTimer is how long the leader will still be the leader.
	// If the quorum of voters is unreachable, the it will step down as follower
	leaseTimer *time.Ticker

	// leaseDuration is used to set leaseTimer ticker
	leaseDuration time.Duration

	// followerReplication hold all requirements that will
	// be used by the leader to replicate append entries
	followerReplication map[string]*followerReplication

	// disableHeartBeat is set to true temporary while sending new entries
	disableHeartBeat atomic.Bool
}

// init initialize all requirements needed by
// the current node type
func (r *leader) init() {
	r.rafty.setLeader(leaderMap{id: r.rafty.id, address: r.rafty.Address.String()})
	r.rafty.leaderLost.Store(false)
	r.rafty.leaderLastContactDate.Store(time.Now())
	r.rafty.nextIndex.Store(r.rafty.lastLogIndex.Load() + 1)
	r.leaseDuration = r.rafty.heartbeatTimeout()
	r.leaseTimer = time.NewTicker(r.leaseDuration * 3)

	r.setupFollowersReplicationStates()

	// heartBeatTimeout is divided by 2 for the leader
	// otherwise it will step down quickly as new election campain
	// will be started by followers
	r.rafty.timer.Reset(r.rafty.heartbeatTimeout() / 2)
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *leader) onTimeout() {
	if r.rafty.getState() != Leader {
		return
	}
	r.heartbeat()
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *leader) release() {
	r.leaseTimer.Stop()
	r.rafty.setLeader(leaderMap{})
	r.stopAllReplication()
	r.wg.Wait()
}

// setupFollowersReplicationStates is build by the leader
// It will create all requirements to replicate
// append entries for the followers
func (r *leader) setupFollowersReplicationStates() {
	r.followerReplication = make(map[string]*followerReplication)
	followers, totalFollowers := r.rafty.getPeers()

	replicationInitialized := make(chan struct{}, totalFollowers)
	for _, follower := range followers {
		followerRepl := &followerReplication{
			peer:                   follower,
			rafty:                  r.rafty,
			newEntryChan:           make(chan *onAppendEntriesRequest, 1),
			replicationInitialized: replicationInitialized,
			replicationStopChan:    make(chan struct{}, 1),
			wg:                     &r.wg,
		}
		r.addReplication(followerRepl)
	}

	// wait for all replication to be initialized
	var replicationCounter atomic.Uint64
	for int(replicationCounter.Load()) != totalFollowers {
		<-replicationInitialized
		replicationCounter.Add(1)
	}

	currentTerm := r.rafty.currentTerm.Load()
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logCommand),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   nil,
		},
	}

	respLog := r.rafty.logs.appendEntries(entries)
	request := &onAppendEntriesRequest{
		totalFollowers: uint64(totalFollowers),
		quorum:         uint64(r.rafty.quorum()),
		term:           currentTerm,
		prevLogIndex:   r.rafty.lastLogIndex.Load(),
		prevLogTerm:    r.rafty.lastLogTerm.Load(),
		totalLogs:      uint64(respLog.total),
		uuid:           uuid.NewString(),
		commitIndex:    r.rafty.commitIndex.Load(),
		entries:        entries,
		catchup:        true,
	}

	r.disableHeartBeat.Store(true)
	defer r.disableHeartBeat.Store(false)
	for _, follower := range followers {
		r.followerReplication[follower.ID].newEntryChan <- request
	}
}

// addReplication add a new follower replication with provided config
func (r *leader) addReplication(follower *followerReplication) {
	follower.nextIndex.Store(1)
	r.followerReplication[follower.ID] = follower
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		follower.startFollowerReplication()
		r.stopReplication(follower, true)
	}()
}

func (r *leader) heartbeat() {
	currentTerm := r.rafty.currentTerm.Load()
	followers, _ := r.rafty.getPeers()
	totalFollowers := len(followers)
	totalLogs := r.rafty.logs.total().total

	request := &onAppendEntriesRequest{
		totalFollowers: uint64(totalFollowers),
		quorum:         uint64(r.rafty.quorum()),
		term:           currentTerm,
		prevLogIndex:   r.rafty.lastLogIndex.Load(),
		prevLogTerm:    r.rafty.lastLogTerm.Load(),
		heartbeat:      true,
		totalLogs:      uint64(totalLogs),
		uuid:           uuid.NewString(),
		commitIndex:    r.rafty.commitIndex.Load(),
	}

	for _, follower := range followers {
		if r.followerReplication[follower.ID] != nil && (!r.followerReplication[follower.ID].replicationStopped.Load() || !r.disableHeartBeat.Load()) {
			r.followerReplication[follower.ID].newEntryChan <- request
		}
	}
}

// stopReplication will stop ongoing follower replication. When deferred is set to true,
// it will decrement waitGroup
func (r *leader) stopReplication(follower *followerReplication, deferred bool) {
	if deferred {
		defer r.wg.Done()
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if !follower.replicationStopped.Load() {
		follower.replicationStopped.Store(true)
		follower.replicationStopChan <- struct{}{}
		r.rafty.Logger.Trace().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", follower.address.String()).
			Str("peerId", follower.ID).
			Msgf("Replication stopped")

		time.Sleep(time.Second)
		close(follower.replicationStopChan)
		close(follower.newEntryChan)
		follower.newEntryChan = nil
	}
	r.rafty.Logger.Info().Str("state", r.rafty.getState().String()).Msgf("DONE stopReplication")
}

// stopAllReplication will stop or force stop all ongoing replication
// and close related chans
func (r *leader) stopAllReplication() {
	for _, follower := range r.followerReplication {
		if follower != nil {
			r.stopReplication(follower, false)
		}
	}
	r.followerReplication = nil
}

// handleAppendEntriesFromClients is used to handle commands
// from clients
func (r *leader) handleAppendEntriesFromClients(kind string, datai any) {
	followers := r.rafty.configuration.ServerMembers
	totalFollowers := len(followers)
	currentTerm := r.rafty.currentTerm.Load()
	var request *onAppendEntriesRequest

	switch kind {
	case "trigger":
		data := datai.(triggerAppendEntries)
		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logCommand),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   data.command,
			},
		}

		respLog := r.rafty.logs.appendEntries(entries)
		request = &onAppendEntriesRequest{
			totalFollowers:    uint64(totalFollowers),
			quorum:            uint64(r.rafty.quorum()),
			term:              currentTerm,
			prevLogIndex:      r.rafty.lastLogIndex.Load(),
			prevLogTerm:       r.rafty.lastLogTerm.Load(),
			totalLogs:         uint64(respLog.total),
			uuid:              uuid.NewString(),
			replyToClient:     true,
			replyToClientChan: data.responseChan,
			commitIndex:       r.rafty.commitIndex.Load(),
			entries:           entries,
			catchup:           true,
		}

	case "forwardCommand":
		data := datai.(forwardCommandToLeaderRequestWrapper)
		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logCommand),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   data.request.Command,
			},
		}

		respLog := r.rafty.logs.appendEntries(entries)
		request = &onAppendEntriesRequest{
			totalFollowers:              uint64(totalFollowers),
			quorum:                      uint64(r.rafty.quorum()),
			term:                        currentTerm,
			prevLogIndex:                r.rafty.lastLogIndex.Load(),
			prevLogTerm:                 r.rafty.lastLogTerm.Load(),
			totalLogs:                   uint64(respLog.total),
			uuid:                        uuid.NewString(),
			replyToForwardedCommand:     true,
			replyToForwardedCommandChan: data.responseChan,
			commitIndex:                 r.rafty.commitIndex.Load(),
			entries:                     entries,
			catchup:                     true,
		}
	}

	r.disableHeartBeat.Store(true)
	defer r.disableHeartBeat.Store(false)
	for _, follower := range followers {
		if r.followerReplication[follower.ID] != nil && !r.followerReplication[follower.ID].replicationStopped.Load() {
			r.followerReplication[follower.ID].newEntryChan <- request
		}
	}
}

// leasing will check if the leader must keep its state or step down
// as follower when the quorum of voters is unreachable
func (r *leader) leasing() {
	if r.rafty.getState() == Leader {
		var unreachable int
		followers, _ := r.rafty.getPeers()
		var newLease time.Duration
		now := time.Now()
		for _, followerConfig := range followers {
			follower := r.followerReplication[followerConfig.ID]
			if follower != nil && !follower.replicationStopped.Load() && !follower.ReadOnlyNode {
				lastContact := follower.lastContactDate.Load()
				if lastContact != nil {
					since := now.Sub(lastContact.(time.Time))
					if r.leaseDuration*3 > since {
						if since > newLease {
							newLease = since
						}
					}
				}
				if follower.failures.Load() >= replicationMaxRetry {
					unreachable++
				}
			}
		}

		quorum := r.rafty.quorum()
		if unreachable >= quorum {
			r.rafty.Logger.Trace().
				Str("address", r.rafty.Address.String()).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("unreachable", fmt.Sprintf("%d", unreachable)).
				Str("quorum", fmt.Sprintf("%d", quorum)).
				Msgf("Quorum unreachable")

			r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
			return
		}
		// we to that to prevent non-positive interval for Ticker.Reset
		// and having a new lease too low is not recommended
		// so we resetted to max value
		max := 500 * time.Millisecond
		if newLease > max || newLease < 50*time.Millisecond {
			newLease = max
		}
		r.leaseDuration = newLease
		r.leaseTimer.Reset(newLease)
	}
}
