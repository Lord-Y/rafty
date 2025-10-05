package rafty

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
)

// init initialize all requirements needed by
// the current node type
func (r *leader) init() {
	r.rafty.metrics.setNodeStateGauge(Leader)
	r.rafty.setLeader(leaderMap{id: r.rafty.id, address: r.rafty.Address.String()})
	r.rafty.leaderLastContactDate.Store(time.Now())
	r.rafty.nextIndex.Store(r.rafty.lastLogIndex.Load() + 1)
	r.leaseDuration = r.rafty.heartbeatTimeout()
	r.leaseTimer = time.NewTicker(r.leaseDuration * 3)
	r.commitChan = make(chan commitChanConfig)
	r.commitIndexWatcher = make(map[uint64]*indexWatcher)
	go r.commitLoop()

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
		r.leaseTimer.Stop()
		r.rafty.setLeader(leaderMap{})
		return
	}

	r.timeoutNowRequest()
	r.leaseTimer.Stop()
	r.rafty.setLeader(leaderMap{})
	r.stopAllReplication()
}

// setupFollowersReplicationStates is build by the leader.
// It will create all requirements to replicate
// append entries for the followers
func (r *leader) setupFollowersReplicationStates() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.followerReplication = make(map[string]*followerReplication)
	followers, totalFollowers := r.rafty.getPeers()
	r.totalFollowers.Store(uint64(totalFollowers))

	for _, follower := range followers {
		followerRepl := &followerReplication{
			Peer:         follower,
			rafty:        r.rafty,
			newEntryChan: make(chan bool),
			notifyLeader: r.commitChan,
		}
		r.addReplication(followerRepl, true)
	}

	r.replicateLog(replicateLogConfig{
		logType: LogNoop,
		command: nil,
	})
}

// addReplication add a new follower replication with provided config.
// updateNextIndex must be set to true when promoting new node
func (r *leader) addReplication(follower *followerReplication, updateNextIndex bool) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	if updateNextIndex {
		follower.nextIndex.Store(1)
	}
	r.followerReplication[follower.ID] = follower
	go follower.startStopFollowerReplication()
}

// heartbeat is used by the leader to send hearbeat entries
// in order to do not lost leadership
func (r *leader) heartbeat() {
	r.replicateLog(replicateLogConfig{heartbeat: true})
}

func (r *leader) isReplicableForHearbeat(follower *followerReplication) bool {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	return r.rafty.getState() == Leader && r.rafty.isRunning.Load() && follower != nil && !follower.catchup.Load() && (!follower.replicationStopped.Load() && !follower.WaitToBePromoted || !r.disableHeartBeat.Load())
}

// isReplicable will check if node is replicable when adding new logs
func (r *leader) isReplicable(follower *followerReplication) bool {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	return r.rafty.getState() == Leader && r.rafty.isRunning.Load() && follower != nil && !follower.replicationStopped.Load() && !follower.WaitToBePromoted
}

// stopReplication will stop ongoing follower replication. When deferred is set to true,
// it will decrement waitGroup. deferred set to true must ONLY used by addReplication func
func (r *leader) stopReplication(follower *followerReplication) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if follower != nil && !follower.replicationStopped.Load() {
		follower.replicationStopped.Store(true)
		delete(r.followerReplication, follower.ID)
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

// replicateLog is used to write the new log locally
// to later notify followerReplication loop to be replicated
// to followers.
func (r *leader) replicateLog(config replicateLogConfig) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if config.heartbeat {
		for _, follower := range r.followerReplication {
			if r.isReplicableForHearbeat(follower) {
				follower.newEntryChan <- true
			}
		}
		return
	}

	if config.logType == LogReplication || config.logType == LogNoop {
		entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, config.command)
		logs := []*LogEntry{entry}
		r.rafty.storeLogs(logs)

		indexWatcher := &indexWatcher{
			totalFollowers: r.totalFollowers.Load(),
			quorum:         uint64(r.rafty.quorum()),
			term:           entry.Term,
			logType:        config.logType,
			uuid:           uuid.NewString(),
			logs:           logs,
			source:         config.source,
			client:         config.client,
			clientChan:     config.clientChan,
		}

		// this is a special setup for single server cluster
		// to avoid duplicate code
		if r.rafty.options.IsSingleServerCluster {
			indexWatcher.totalFollowers = 1
			indexWatcher.quorum = 2
			r.murw.Lock()
			r.commitIndexWatcher[entry.Index] = indexWatcher
			r.murw.Unlock()
			r.commitChan <- commitChanConfig{id: r.rafty.id, matchIndex: entry.Index}
			return
		}

		r.murw.Lock()
		r.commitIndexWatcher[entry.Index] = indexWatcher
		r.murw.Unlock()

		for _, follower := range r.followerReplication {
			if r.isReplicable(follower) {
				follower.newEntryChan <- false
			}
		}
		return
	}

	// logConfiguration section
	if r.rafty.membershipChangeInProgress.Load() {
		response := RPCResponse{
			Response: &raftypb.AppendEntryResponse{},
			Error:    ErrMembershipChangeInProgress,
		}
		if config.source == "forward" {
			response = RPCResponse{
				Response: &raftypb.ForwardCommandToLeaderResponse{},
				Error:    ErrMembershipChangeInProgress,
			}
		}

		select {
		case <-r.rafty.quitCtx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			return
		case config.clientChan <- response:
		}
		return
	}

	r.rafty.membershipChangeInProgress.Store(true)
	defer r.rafty.membershipChangeInProgress.Store(false)

	switch config.membershipChange.action {
	case Add:
		followerMember := &followerReplication{
			Peer:         config.membershipChange.member,
			rafty:        r.rafty,
			newEntryChan: make(chan bool),
			notifyLeader: r.commitChan,
		}

		var progress atomic.Bool
		progress.Store(true)
		result, _ := r.rafty.logStore.GetLogByIndex(r.rafty.matchIndex.Load())
		request := &onAppendEntriesRequest{
			term:                       r.rafty.currentTerm.Load(),
			prevLogIndex:               result.Index,
			prevLogTerm:                result.Term,
			catchup:                    true,
			rpcTimeout:                 time.Second,
			membershipChangeInProgress: &progress,
			membershipChangeID:         config.membershipChange.member.ID,
		}

		buildResponse := &raftypb.AppendEntryResponse{
			LogNotFound: true,
		}

		// if the member is not part of the cluster
		if !r.rafty.isPartOfTheCluster(config.membershipChange.member) {
			encodedPeers, _ := r.rafty.validateSendMembershipChangeRequest(config.membershipChange.action, config.membershipChange.member)
			entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, encodedPeers)
			logs := []*LogEntry{entry}
			r.rafty.storeLogs(logs)

			r.murw.Lock()
			r.commitIndexWatcher[entry.Index] = &indexWatcher{
				totalFollowers:   r.totalFollowers.Load(),
				quorum:           uint64(r.rafty.quorum()),
				term:             entry.Term,
				logType:          config.logType,
				uuid:             uuid.NewString(),
				logs:             logs,
				source:           config.source,
				client:           config.client,
				clientChan:       config.clientChan,
				membershipChange: config.membershipChange,
			}
			r.murw.Unlock()

			_ = r.rafty.applyConfigEntry(makeProtobufLogEntry(entry)[0])
			if err := r.rafty.clusterStore.StoreMetadata(r.rafty.buildMetadata()); err != nil {
				panic(err)
			}

			for _, follower := range r.followerReplication {
				if r.isReplicable(follower) {
					follower.newEntryChan <- false
				}
			}
		}

		if err := followerMember.catchupNewMember(config.membershipChange.member, request, buildResponse); err != nil {
			return
		}

		// auto promote node
		encodedPeers, _ := r.rafty.validateSendMembershipChangeRequest(Promote, config.membershipChange.member)
		entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, encodedPeers)
		logs := []*LogEntry{entry}
		r.rafty.storeLogs(logs)

		config.membershipChange.action = Promote
		r.murw.Lock()
		r.commitIndexWatcher[entry.Index] = &indexWatcher{
			totalFollowers:   r.totalFollowers.Load(),
			quorum:           uint64(r.rafty.quorum()),
			term:             entry.Term,
			logType:          config.logType,
			uuid:             uuid.NewString(),
			logs:             logs,
			source:           config.source,
			client:           config.client,
			clientChan:       config.clientChan,
			membershipChange: config.membershipChange,
		}
		r.murw.Unlock()

		_ = r.rafty.applyConfigEntry(makeProtobufLogEntry(entry)[0])
		if err := r.rafty.clusterStore.StoreMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}

		for _, follower := range r.followerReplication {
			if r.isReplicable(follower) {
				follower.newEntryChan <- false
			}
		}

		r.addReplication(followerMember, false)
		r.totalFollowers.Add(1)

	case Promote:
		followerMember := &followerReplication{
			Peer:         config.membershipChange.member,
			rafty:        r.rafty,
			newEntryChan: make(chan bool),
			notifyLeader: r.commitChan,
		}

		encodedPeers, _ := r.rafty.validateSendMembershipChangeRequest(config.membershipChange.action, config.membershipChange.member)
		entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, encodedPeers)
		logs := []*LogEntry{entry}
		r.rafty.storeLogs(logs)

		r.murw.Lock()
		r.commitIndexWatcher[entry.Index] = &indexWatcher{
			totalFollowers:   r.totalFollowers.Load(),
			quorum:           uint64(r.rafty.quorum()),
			term:             entry.Term,
			logType:          config.logType,
			uuid:             uuid.NewString(),
			logs:             logs,
			source:           config.source,
			client:           config.client,
			clientChan:       config.clientChan,
			membershipChange: config.membershipChange,
		}
		r.murw.Unlock()

		_ = r.rafty.applyConfigEntry(makeProtobufLogEntry(entry)[0])
		if err := r.rafty.clusterStore.StoreMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}

		for _, follower := range r.followerReplication {
			if r.isReplicable(follower) {
				follower.newEntryChan <- false
			}
		}

		r.addReplication(followerMember, false)
		r.totalFollowers.Add(1)

	case Demote:
		encodedPeers, err := r.rafty.validateSendMembershipChangeRequest(config.membershipChange.action, config.membershipChange.member)
		if err != nil {
			response := RPCResponse{
				Response: &raftypb.AppendEntryResponse{},
				Error:    err,
			}
			if config.source == "forward" {
				response = RPCResponse{
					Response: &raftypb.ForwardCommandToLeaderResponse{},
					Error:    err,
				}
			}

			select {
			case <-r.rafty.quitCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				return
			case config.clientChan <- response:
			}
			return
		}

		entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, encodedPeers)
		logs := []*LogEntry{entry}
		r.rafty.storeLogs(logs)

		r.murw.Lock()
		r.commitIndexWatcher[entry.Index] = &indexWatcher{
			totalFollowers:   r.totalFollowers.Load(),
			quorum:           uint64(r.rafty.quorum()),
			term:             entry.Term,
			logType:          config.logType,
			uuid:             uuid.NewString(),
			logs:             logs,
			source:           config.source,
			client:           config.client,
			clientChan:       config.clientChan,
			membershipChange: config.membershipChange,
		}
		r.murw.Unlock()

		_ = r.rafty.applyConfigEntry(makeProtobufLogEntry(entry)[0])
		if err := r.rafty.clusterStore.StoreMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}

		for _, follower := range r.followerReplication {
			if r.isReplicable(follower) {
				follower.newEntryChan <- false
			}
		}

	case Remove, ForceRemove, LeaveOnTerminate:
		encodedPeers, err := r.rafty.validateSendMembershipChangeRequest(config.membershipChange.action, config.membershipChange.member)
		if err != nil {
			response := RPCResponse{
				Response: &raftypb.AppendEntryResponse{},
				Error:    err,
			}
			if config.source == "forward" {
				response = RPCResponse{
					Response: &raftypb.ForwardCommandToLeaderResponse{},
					Error:    err,
				}
			}

			select {
			case <-r.rafty.quitCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				return
			case config.clientChan <- response:
			}
			return
		}

		entry := makeNewLogEntry(r.rafty.currentTerm.Load(), config.logType, encodedPeers)
		logs := []*LogEntry{entry}
		r.rafty.storeLogs(logs)

		r.murw.Lock()
		r.commitIndexWatcher[entry.Index] = &indexWatcher{
			totalFollowers:   r.totalFollowers.Load(),
			quorum:           uint64(r.rafty.quorum()),
			term:             entry.Term,
			logType:          config.logType,
			uuid:             uuid.NewString(),
			logs:             logs,
			source:           config.source,
			client:           config.client,
			clientChan:       config.clientChan,
			membershipChange: config.membershipChange,
		}
		r.murw.Unlock()

		_ = r.rafty.applyConfigEntry(makeProtobufLogEntry(entry)[0])
		if err := r.rafty.clusterStore.StoreMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}

		for _, follower := range r.followerReplication {
			if config.membershipChange.action == LeaveOnTerminate && follower != nil && follower.ID == config.membershipChange.member.ID {
				continue
			}
			if r.isReplicable(follower) {
				follower.newEntryChan <- false
			}
		}

		r.totalFollowers.Store(r.totalFollowers.Load() - 1)
		for _, follower := range r.followerReplication {
			if follower != nil && follower.ID == config.membershipChange.member.ID {
				r.stopReplication(follower)
				break
			}
		}
	}
}

// commitLoop is in charge of advancing leader commitIndex
// based on followers matchIndex.
func (r *leader) commitLoop() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	for r.rafty.getState() == Leader {
		select {
		case <-r.rafty.quitCtx.Done():
			return

		case data, ok := <-r.commitChan:
			if ok {
				r.murw.Lock()
				if watcher, ok := r.commitIndexWatcher[data.matchIndex]; ok {
					r.commitIndexWatcher[data.matchIndex].majority.Add(1)

					if r.commitIndexWatcher[data.matchIndex].majority.Load()+1 >= watcher.quorum {
						if !watcher.committed.Load() {
							watcher.committed.Store(true)
							// uptdate leader volatile state
							r.rafty.nextIndex.Add(1)
							r.rafty.matchIndex.Add(1)
							r.rafty.commitIndex.Add(1)

							if watcher.client {
								response := RPCResponse{
									Response: &raftypb.AppendEntryResponse{},
								}

								if watcher.source == "forward" {
									response = RPCResponse{
										Response: &raftypb.ForwardCommandToLeaderResponse{
											LeaderId:      r.rafty.id,
											LeaderAddress: r.rafty.Address.String(),
										},
									}
								}

								select {
								case <-time.After(100 * time.Millisecond):
									return
								case watcher.clientChan <- response:
								}
							}

							if watcher.logType == LogReplication {
								if _, err := r.rafty.applyLogs(applyLogs{makeProtobufLogEntries(watcher.logs)}); err != nil {
									r.rafty.Logger.Error().Err(err).
										Str("id", r.rafty.id).
										Str("state", r.rafty.getState().String()).
										Str("term", fmt.Sprintf("%d", watcher.term)).
										Msgf("Fail to apply log entries to the fsm")
								}
							}

							r.rafty.Logger.Trace().
								Str("address", r.rafty.Address.String()).
								Str("id", r.rafty.id).
								Str("state", r.rafty.getState().String()).
								Str("term", fmt.Sprintf("%d", watcher.term)).
								Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
								Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
								Str("commitIndex", fmt.Sprintf("%d", r.rafty.commitIndex.Load())).
								Str("lastApplied", fmt.Sprintf("%d", r.rafty.lastApplied.Load())).
								Str("requestUUID", watcher.uuid).
								Msgf("Leader volatile state has been updated")

							if watcher.logType == LogConfiguration {
								// if it's myself
								if r.rafty.id == watcher.membershipChange.member.ID {
									switch watcher.membershipChange.action {
									case Remove, ForceRemove:
										if r.rafty.options.ShutdownOnRemove {
											go r.rafty.stop()
											return
										}

									case Demote:
										r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
										return
									}
								}
							}
						}
					}

					if watcher.totalFollowers == watcher.majority.Load() {
						delete(r.commitIndexWatcher, data.matchIndex)
					}
				}
				r.murw.Unlock()
			}
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
func (r *leader) selectNodeForLeadershipTransfer() (p Peer, found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, follower := range r.followerReplication {
		if follower != nil && !follower.ReadReplica {
			r.rafty.Logger.Trace().
				Str("address", r.rafty.Address.String()).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("peerAddress", follower.Peer.address.String()).
				Str("peerId", follower.Peer.ID).
				Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
				Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
				Str("peerNextIndex", fmt.Sprintf("%d", follower.nextIndex.Load())).
				Str("peerMatchIndex", fmt.Sprintf("%d", follower.matchIndex.Load())).
				Str("leadershipTransferDisabled", fmt.Sprintf("%t", r.rafty.leadershipTransferDisabled.Load())).
				Msgf("LeadershipTransfer select suitable node")

			if r.rafty.matchIndex.Load() == follower.matchIndex.Load() {
				p = follower.Peer
				found = true
				break
			}
		}
	}
	return
}

// timeoutNowRequest is used by the leader to send
// its leadership to the select node
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
			client := r.rafty.connectionManager.getClient(peer.address.String())
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
				r.leadershipTransferInProgress.Store(true)
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

// setupSingleServerReplicationState is build by the leader.
// It will create all requirements to append entries to its logs
func (r *leader) setupSingleServerReplicationState() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	r.totalFollowers.Store(1)
	r.replicateLog(replicateLogConfig{
		logType: LogNoop,
		command: nil,
	})
}
