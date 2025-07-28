package rafty

// commonLoop handle all request that all
// nodes have in common
func (r *Rafty) commonLoop() {
	r.wg.Add(1)
	defer r.wg.Done()

	for {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainGetLeaderResult()
			r.drainSendAskNodeIDRequests()
			r.drainBootstrapClusterRequests()
			return

		// handle client get leader
		case data := <-r.rpcClientGetLeaderChan:
			r.getLeaderResult(data)

		// handle ask node id
		case data := <-r.rpcAskNodeIDChan:
			r.askNodeIDResult(data)

		// handle cluster bootstrap
		case data := <-r.rpcBootstrapClusterRequestChan:
			r.bootstrapCluster(data)
		}
	}
}

// stateLoop handle all needs required by all kind of nodes
func (r *Rafty) stateLoop() {
	for r.getState() != Down {
		switch r.getState() {
		case ReadReplica:
			r.runAsReadReplica()
		case Follower:
			r.runAsFollower()
		case Candidate:
			r.runAsCandidate()
		case Leader:
			r.runAsLeader()
		}
	}
}

// runAsReadReplica will run node as read replica
func (r *Rafty) runAsReadReplica() {
	r.wg.Add(1)
	defer r.wg.Done()

	state := readReplica{rafty: r}
	defer state.release()
	state.init()

	for r.getState() == ReadReplica {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainPreVoteRequests()
			r.drainVoteRequests()
			r.drainAppendEntriesRequests()
			return

		// common state timer
		case <-r.timer.C:
			state.onTimeout()

		// handle append entries from the leader
		case data, ok := <-r.rpcAppendEntriesRequestChan:
			if ok {
				r.handleSendAppendEntriesRequest(data)
			}

		// handle membership change response from leader
		case data, ok := <-r.rpcMembershipChangeChan:
			if ok {
				r.membershipChangeResponse(data)
			}

		// handle membership sent when current node is NOT the leader
		case data, ok := <-r.rpcMembershipChangeRequestChan:
			if ok {
				r.rpcMembershipNotLeader(data)
			}
		}
	}
}

// runAsFollower will run node as follower
func (r *Rafty) runAsFollower() {
	r.wg.Add(1)
	defer r.wg.Done()

	state := follower{rafty: r}
	defer state.release()
	state.init()

	for r.getState() == Follower {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainPreVoteRequests()
			r.drainVoteRequests()
			r.drainAppendEntriesRequests()
			return

		// common state timer
		case <-r.timer.C:
			state.onTimeout()

		// receive and answer pre vote requests from other nodes
		case data, ok := <-r.rpcPreVoteRequestChan:
			if ok {
				r.handleSendPreVoteRequest(data)
			}

		// receive and answer request vote from other nodes
		case data, ok := <-r.rpcVoteRequestChan:
			if ok {
				r.handleSendVoteRequest(data)
			}

		// handle append entries from the leader
		case data, ok := <-r.rpcAppendEntriesRequestChan:
			if ok {
				r.handleSendAppendEntriesRequest(data)
			}

		// handle membership change response from leader
		case data, ok := <-r.rpcMembershipChangeChan:
			if ok {
				r.membershipChangeResponse(data)
			}

		// handle membership sent when current node is NOT the leader
		case data, ok := <-r.rpcMembershipChangeRequestChan:
			if ok {
				r.rpcMembershipNotLeader(data)
			}
		}
	}
}

// runAsCandidate will run node as candidate
func (r *Rafty) runAsCandidate() {
	r.wg.Add(1)
	defer r.wg.Done()

	state := candidate{rafty: r}
	defer state.release()
	state.init()

	for r.getState() == Candidate {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainPreVoteRequests()
			r.drainVoteRequests()
			r.drainAppendEntriesRequests()
			return

		// common state timer
		case <-r.timer.C:
			state.onTimeout()

		// receive and answer pre vote requests from other nodes
		case data, ok := <-r.rpcPreVoteRequestChan:
			if ok {
				r.handleSendPreVoteRequest(data)
			}

		// receive and answer request vote from other nodes
		case data, ok := <-r.rpcVoteRequestChan:
			if ok {
				r.handleSendVoteRequest(data)
			}

			// handle pre vote response from other nodes
		case data, ok := <-state.responsePreVoteChan:
			if ok {
				state.handlePreVoteResponse(data)
			}

		// handle vote response from other nodes
		// and become a leader if conditions are met
		case data, ok := <-state.responseVoteChan:
			if ok {
				state.handleVoteResponse(data)
			}

		// handle append entries from the leader
		case data, ok := <-r.rpcAppendEntriesRequestChan:
			if ok {
				r.handleSendAppendEntriesRequest(data)
			}

		// handle membership sent when current node is NOT the leader
		case data, ok := <-r.rpcMembershipChangeRequestChan:
			if ok {
				r.rpcMembershipNotLeader(data)
			}
		}
	}
}

// runAsLeader will run node as leader
func (r *Rafty) runAsLeader() {
	r.wg.Add(1)
	defer r.wg.Done()

	state := leader{rafty: r}
	defer state.release()
	state.init()

	for r.getState() == Leader {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainPreVoteRequests()
			r.drainVoteRequests()
			r.drainAppendEntriesRequests()
			r.drainMembershipChangeRequests()
			return

		// common state timer
		case <-r.timer.C:
			state.onTimeout()

		// this chan will check if lease is still valid
		case <-state.leaseTimer.C:
			state.leasing()

		// receive and answer pre vote requests from other nodes
		case data, ok := <-r.rpcPreVoteRequestChan:
			if ok {
				r.handleSendPreVoteRequest(data)
			}

		// receive and answer request vote from other nodes
		case data, ok := <-r.rpcVoteRequestChan:
			if ok {
				r.handleSendVoteRequest(data)
			}

		// handle append entries from the leader
		case data, ok := <-r.rpcAppendEntriesRequestChan:
			if ok {
				r.handleSendAppendEntriesRequest(data)
			}

		// this chan is used by clients to apply commands on the leader
		case data, ok := <-r.triggerAppendEntriesChan:
			if ok {
				state.handleAppendEntriesFromClients("trigger", data)
			}

		// commands sent by clients to Follower nodes will be forwarded to the leader
		// to later apply commands
		case data, ok := <-r.rpcForwardCommandToLeaderRequestChan:
			if ok {
				state.handleAppendEntriesFromClients("forwardCommand", data)
			}

		// handle membership sent by follower nodes to the leader
		case data, ok := <-r.rpcMembershipChangeRequestChan:
			if ok {
				state.handleSendMembershipChangeRequest(data)
			}
		}
	}
}

// release will stop everything necessary to shut down the node
func (r *Rafty) release() {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	r.connectionManager.disconnectAllPeers()
}
