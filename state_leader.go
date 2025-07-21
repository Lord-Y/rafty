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

	// totalFollowers is the total number of followers
	// used during the replication. It must be update when adding or removing
	// a follower
	totalFollowers atomic.Uint64

	// disableHeartBeat is set to true temporary while sending new entries
	disableHeartBeat atomic.Bool

	// leadershipTransferTimer is used for leadership transfer
	leadershipTransferTimer *time.Ticker

	// leadershipTransferDuration is a helper used by leadershipTransferTimer
	leadershipTransferDuration time.Duration

	// leadershipTransferChan will receive rpc response
	leadershipTransferChan chan RPCResponse

	// leadershipTransferChanClosed is a helper telling us if leadershipTransferChan is closed
	// to stop leadershipTransferLoop func
	leadershipTransferChanClosed atomic.Bool

	// leadershipTransferInProgress is used to check if a leadership transfer is in progress
	// an is set to true when leadershipTransferTimer is resetted.
	// When leadershipTransferTimer times out, the leadership transfer will be stopped
	leadershipTransferInProgress atomic.Bool

	// membershipChangeInProgress is set to true when membership change is ongoing
	membershipChangeInProgress atomic.Bool

	// singleServerNewEntryChan is used by the leader every times it received
	// a new log entry
	singleServerNewEntryChan chan *onAppendEntriesRequest

	// singleServerReplicationStopChan is used by the leader
	// in order stop ongoing append entries replication
	singleServerReplicationStopChan chan struct{}

	// singleServerReplicationStopped is only a helper to indicate if a singleServerReplicationStopChan is closed
	singleServerReplicationStopped atomic.Bool
}

// init initialize all requirements needed by
// the current node type
func (r *leader) init() {
	r.rafty.setLeader(leaderMap{id: r.rafty.id, address: r.rafty.Address.String()})
	r.rafty.leaderLastContactDate.Store(time.Now())
	r.rafty.nextIndex.Store(r.rafty.lastLogIndex.Load() + 1)
	r.leaseDuration = r.rafty.heartbeatTimeout()
	r.leaseTimer = time.NewTicker(r.leaseDuration * 3)

	if r.rafty.options.IsSingleServerCluster {
		r.setupSingleServerReplicationState()
	} else {
		r.leadershipTransferDuration = r.rafty.heartbeatTimeout()
		r.leadershipTransferTimer = time.NewTicker(r.leadershipTransferDuration)
		r.leadershipTransferChan = make(chan RPCResponse, 1)
		go r.leadershipTransferLoop()

		r.setupFollowersReplicationStates()
	}

	// heartBeatTimeout is divided by 2 for the leader
	// otherwise it will step down quickly as new election campaign
	// will be started by followers
	r.rafty.timer.Reset(r.rafty.heartbeatTimeout() / 2)
}

// onTimeout permit to reset election timer
// and then perform some other actions
func (r *leader) onTimeout() {
	if r.rafty.getState() != Leader || r.rafty.options.IsSingleServerCluster {
		return
	}

	if r.rafty.decommissioning.Load() {
		r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
		return
	}
	r.heartbeat()
}

// release permit to cancel or gracefully some actions
// when the node change state
func (r *leader) release() {
	if r.rafty.options.IsSingleServerCluster {
		if !r.singleServerReplicationStopped.Load() {
			r.singleServerReplicationStopChan <- struct{}{}
		}
		r.leaseTimer.Stop()
		r.rafty.setLeader(leaderMap{})
		return
	}

	r.timeoutNowRequest()
	r.leaseTimer.Stop()
	r.rafty.setLeader(leaderMap{})
	r.stopAllReplication()
}

// setupFollowersReplicationStates is build by the leader
// It will create all requirements to replicate
// append entries for the followers
func (r *leader) setupFollowersReplicationStates() {
	r.followerReplication = make(map[string]*followerReplication)
	followers, totalFollowers := r.rafty.getPeers()
	r.totalFollowers.Store(uint64(totalFollowers))

	for _, follower := range followers {
		followerRepl := &followerReplication{
			peer:                follower,
			rafty:               r.rafty,
			newEntryChan:        make(chan *onAppendEntriesRequest),
			replicationStopChan: make(chan struct{}, 1),
		}
		r.addReplication(followerRepl, true)
	}

	currentTerm := r.rafty.currentTerm.Load()
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logNoop),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   nil,
		},
	}

	totalLogs := r.rafty.logs.appendEntries(entries, false)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  uint64(totalLogs),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &atomic.Bool{},
	}

	r.disableHeartBeat.Store(true)
	defer r.disableHeartBeat.Store(false)
	for _, follower := range r.followerReplication {
		if r.rafty.getState() == Leader && r.rafty.isRunning.Load() {
			follower.newEntryChan <- request
		}
	}
}

// addReplication add a new follower replication with provided config.
// updateNextIndex must be set to true when promoting new node
func (r *leader) addReplication(follower *followerReplication, updateNextIndex bool) {
	if updateNextIndex {
		follower.nextIndex.Store(1)
	}
	r.followerReplication[follower.ID] = follower
	go follower.startStopFollowerReplication()
}

// heartbeat is used by the leader to send hearbeat entries
// in order to do not lost leadership
func (r *leader) heartbeat() {
	currentTerm := r.rafty.currentTerm.Load()
	totalLogs := r.rafty.logs.total().total

	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		heartbeat:                  true,
		totalLogs:                  uint64(totalLogs),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &atomic.Bool{},
	}

	for _, follower := range r.followerReplication {
		if r.isReplicableForHearbeat(follower) {
			follower.newEntryChan <- request
		}
	}
}

// isReplicableForHearbeat will check if node is replicable by heartbeat process
func (r *leader) isReplicableForHearbeat(follower *followerReplication) bool {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	return r.rafty.getState() == Leader && follower != nil && (!follower.replicationStopped.Load() && !follower.WaitToBePromoted || !r.disableHeartBeat.Load() || r.rafty.isRunning.Load())
}

// isReplicable will check if node is replicable when adding new logs
func (r *leader) isReplicable(follower *followerReplication) bool {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	return r.rafty.getState() == Leader && follower != nil && (!follower.replicationStopped.Load() && !follower.WaitToBePromoted || r.rafty.isRunning.Load())
}

// stopReplication will stop ongoing follower replication. When deferred is set to true,
// it will decrement waitGroup. deferred set to true must ONLY used by addReplication func
func (r *leader) stopReplication(follower *followerReplication) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if follower != nil && !follower.replicationStopped.Load() {
		follower.replicationStopChan <- struct{}{}
	}
}

// stopAllReplication will stop or force stop all ongoing replication
// and close related chans
func (r *leader) stopAllReplication() {
	for _, follower := range r.followerReplication {
		if follower != nil {
			r.stopReplication(follower)
		}
	}
	r.followerReplication = nil
}

// handleAppendEntriesFromClients is used to handle commands
// from clients
func (r *leader) handleAppendEntriesFromClients(kind string, datai any) {
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

		totalLogs := r.rafty.logs.appendEntries(entries, false)
		request = &onAppendEntriesRequest{
			totalFollowers:             r.totalFollowers.Load(),
			quorum:                     uint64(r.rafty.quorum()),
			term:                       currentTerm,
			prevLogIndex:               r.rafty.lastLogIndex.Load(),
			prevLogTerm:                r.rafty.lastLogTerm.Load(),
			totalLogs:                  uint64(totalLogs),
			uuid:                       uuid.NewString(),
			replyToClient:              true,
			replyToClientChan:          data.responseChan,
			commitIndex:                r.rafty.commitIndex.Load(),
			entries:                    entries,
			catchup:                    true,
			rpcTimeout:                 r.rafty.randomRPCTimeout(true),
			membershipChangeInProgress: &atomic.Bool{},
		}

	case "forwardCommand":
		data := datai.(RPCRequest)
		drequest := data.Request.(*raftypb.ForwardCommandToLeaderRequest)
		entries := []*raftypb.LogEntry{
			{
				LogType:   uint32(logCommand),
				Timestamp: uint32(time.Now().Unix()),
				Term:      currentTerm,
				Command:   drequest.Command,
			},
		}

		totalLogs := r.rafty.logs.appendEntries(entries, false)
		request = &onAppendEntriesRequest{
			totalFollowers:              r.totalFollowers.Load(),
			quorum:                      uint64(r.rafty.quorum()),
			term:                        currentTerm,
			prevLogIndex:                r.rafty.lastLogIndex.Load(),
			prevLogTerm:                 r.rafty.lastLogTerm.Load(),
			totalLogs:                   uint64(totalLogs),
			uuid:                        uuid.NewString(),
			replyToForwardedCommand:     true,
			replyToForwardedCommandChan: data.ResponseChan,
			commitIndex:                 r.rafty.commitIndex.Load(),
			entries:                     entries,
			catchup:                     true,
			rpcTimeout:                  r.rafty.randomRPCTimeout(true),
			membershipChangeInProgress:  &atomic.Bool{},
		}
	}

	if r.rafty.options.IsSingleServerCluster {
		if r.rafty.getState() == Leader && r.rafty.isRunning.Load() {
			r.singleServerNewEntryChan <- request
		}
		return
	}

	r.disableHeartBeat.Store(true)
	defer r.disableHeartBeat.Store(false)
	for _, follower := range r.followerReplication {
		if r.isReplicable(follower) {
			follower.newEntryChan <- request
		}
	}
}

// leasing will check if the leader must keep its state or step down
// as follower when the quorum of voters is unreachable
func (r *leader) leasing() {
	if r.rafty.getState() == Leader {
		max := 500 * time.Millisecond
		if r.rafty.options.IsSingleServerCluster {
			r.leaseTimer.Reset(max)
			return
		}

		var unreachable int
		var newLease time.Duration
		now := time.Now()
		for _, follower := range r.followerReplication {
			if follower != nil && !follower.replicationStopped.Load() && !follower.ReadReplica {
				lastContact := follower.lastContactDate.Load()
				if lastContact != nil {
					since := now.Sub(lastContact.(time.Time))
					if r.leaseDuration > since {
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

			r.rafty.leadershipTransferDisabled.Store(true)
			r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
			return
		}
		// To prevent non-positive interval for Ticker.Reset
		// and having a new lease too low is not recommended
		// so we resetted to max value
		if newLease > max || newLease < 50*time.Millisecond {
			newLease = max
		}
		r.leaseDuration = newLease
		r.leaseTimer.Reset(newLease)
	}
}

// selectNodeForLeadershipTransfer will return a node that is in sync
// with leader logs
func (r *leader) selectNodeForLeadershipTransfer() (p peer, found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, follower := range r.followerReplication {
		if follower != nil && !follower.ReadReplica {
			r.rafty.Logger.Trace().
				Str("address", r.rafty.Address.String()).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("peerAddress", follower.peer.address.String()).
				Str("peerId", follower.peer.ID).
				Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
				Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
				Str("peerNextIndex", fmt.Sprintf("%d", follower.nextIndex.Load())).
				Str("peerMatchIndex", fmt.Sprintf("%d", follower.matchIndex.Load())).
				Str("leadershipTransferDisabled", fmt.Sprintf("%t", r.rafty.leadershipTransferDisabled.Load())).
				Msgf("LeadershipTransfer select suitable node")

			if r.rafty.matchIndex.Load() == follower.matchIndex.Load() {
				p = follower.peer
				found = true
				break
			}
		}
	}
	return
}

func (r *leader) timeoutNowRequest() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if !r.rafty.leadershipTransferDisabled.Load() {
		r.rafty.leadershipTransferInProgress.Store(true)
		defer r.rafty.leadershipTransferInProgress.Store(false)
		request := RPCRequest{
			RPCType:      TimeoutNowRequest,
			Request:      RPCTimeoutNowRequest{},
			Timeout:      r.rafty.randomRPCTimeout(false),
			ResponseChan: r.leadershipTransferChan,
		}

		peer, found := r.selectNodeForLeadershipTransfer()
		if found {
			client := r.rafty.connectionManager.getClient(peer.address.String(), peer.ID)
			if client != nil {
				r.leadershipTransferTimer.Reset(r.leadershipTransferDuration)
				r.rafty.sendRPC(request, client, peer)
				r.rafty.Logger.Trace().
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Str("peerAddress", peer.address.String()).
					Str("peerId", peer.ID).
					Msgf("LeadershipTransfer initiated")
				close(r.leadershipTransferChan)
				r.leadershipTransferChanClosed.Store(true)
				return
			}
		}
	}
	close(r.leadershipTransferChan)
	r.leadershipTransferChanClosed.Store(true)
}

// leadershipTransferLoop is used to handle leadership transfer
// It will wait for TimeoutNowResponse and then check if the transfer was successful
// If the transfer was successful, it will stop the leadership transfer timer
// If the transfer was not successful, it will close the leadershipTransferChan
// and stop the leadership transfer loop
// If the leadership transfer timer times out, it will close the leadershipTransferChan
// and stop the leadership transfer loop
func (r *leader) leadershipTransferLoop() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	for !r.leadershipTransferChanClosed.Load() {
		select {
		case resp := <-r.leadershipTransferChan:
			r.leadershipTransferTimer.Stop()
			if resp.Response != nil {
				response := resp.Response.(RPCTimeoutNowResponse)
				err := resp.Error
				targetPeer := resp.TargetPeer

				if err != nil {
					r.rafty.Logger.Error().Err(err).
						Str("address", r.rafty.Address.String()).
						Str("id", r.rafty.id).
						Str("state", r.rafty.getState().String()).
						Str("peerAddress", targetPeer.address.String()).
						Str("peerId", targetPeer.ID).
						Str("leadershipTransferChanClosed", fmt.Sprintf("%t", r.leadershipTransferChanClosed.Load())).
						Msgf("Fail to perform leadership transfer to peer")
					return
				}

				if !response.Success {
					r.rafty.Logger.Trace().
						Str("address", r.rafty.Address.String()).
						Str("id", r.rafty.id).
						Str("state", r.rafty.getState().String()).
						Str("peerAddress", targetPeer.address.String()).
						Str("peerId", targetPeer.ID).
						Str("leadershipTransferChanClosed", fmt.Sprintf("%t", r.leadershipTransferChanClosed.Load())).
						Msgf("Fail to perform leadership transfer to peer")
					return
				}
			}

		case <-r.leadershipTransferTimer.C:
			if r.leadershipTransferInProgress.Load() {
				r.leadershipTransferChanClosed.Store(true)
			}
		}
	}
}

func (r *leader) setupSingleServerReplicationState() {
	r.singleServerNewEntryChan = make(chan *onAppendEntriesRequest)
	r.singleServerReplicationStopChan = make(chan struct{}, 1)
	go r.startStopSingleServerReplication()

	currentTerm := r.rafty.currentTerm.Load()
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logNoop),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   nil,
		},
	}

	totalLogs := r.rafty.logs.appendEntries(entries, false)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  uint64(totalLogs),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		membershipChangeInProgress: &atomic.Bool{},
	}

	if r.rafty.getState() == Leader && r.rafty.isRunning.Load() {
		r.singleServerNewEntryChan <- request
	}
}
