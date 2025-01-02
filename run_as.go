package rafty

import (
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *Rafty) runAsFollower() {
	if !r.minimumClusterSizeReach.Load() {
		return
	}
	r.logState(r.getState(), true, r.getCurrentTerm())
	r.mu.Lock()
	r.preVoteElectionTimerEnabled.Store(true)
	preVoteTimeout := r.randomElectionTimeout(true)
	r.preVoteElectionTimer = time.NewTimer(preVoteTimeout)
	// the following is necessary to prevent nil pointer exception when we are follower state
	// and a leader is sending appendEntries
	r.electionTimer = time.NewTimer(r.randomElectionTimeout(false))
	r.mu.Unlock()

	timeout := time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond
	hearbeatTimer := time.NewTimer(timeout)
	myAddress, myId := r.getMyAddress()

	for r.getState() == Follower {
		select {
		case <-r.quitCtx.Done():
			r.switchState(Down, true, r.getCurrentTerm())
			return

		case <-hearbeatTimer.C:
			if r.getState() == Down {
				hearbeatTimer.Stop()
				return
			}
			if !r.startElectionCampain.Load() {
				return
			}
			r.Logger.Trace().Msgf("Me %s / %s with reports timeout %s leaderHeartBeatTimeout %d multiplier %d", myAddress, myId, timeout, leaderHeartBeatTimeout, r.TimeMultiplier)
			hearbeatTimer = time.NewTimer(timeout)
			myAddress, myId := r.getMyAddress()
			r.mu.Lock()
			leaderLastContactDate := r.LeaderLastContactDate
			leaderLost := false
			if leaderLastContactDate != nil && time.Since(*leaderLastContactDate) > timeout {
				r.leaderLost.Store(true)
				leaderLost = true
				if r.leader != nil {
					r.oldLeader = r.leader
					r.Logger.Info().Msgf("Me %s / %s reports that Leader %s / %s has been lost for term %d", myAddress, myId, r.oldLeader.address, r.oldLeader.id, r.getCurrentTerm())
				}
				r.leader = nil
				r.votedFor = ""
			}
			r.mu.Unlock()

			if leaderLost && r.getState() != Down {
				if !r.preVoteElectionTimerEnabled.Load() {
					r.startElectionCampain.Store(false)
					r.resetElectionTimer(true, false)
				}
			}

		// start pre vote request
		case <-r.preVoteElectionTimer.C:
			if !r.preVoteElectionTimerEnabled.Load() {
				return
			}
			r.Logger.Trace().Msgf("Me %s / %s reports heartbeat preVoteTimeout %s", myAddress, myId, preVoteTimeout)
			r.preVoteRequest()

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if r.getLeader() != nil {
				return
			}
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle vote response from other nodes
		// and become a leader if conditions are met
		case vote := <-r.voteResponseChan:
			r.handleVoteResponse(vote)

		// handle vote response error from other nodes
		case voteError := <-r.voteResponseErrorChan:
			r.handleVoteResponseError(voteError)

		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()
		}
	}
}

func (r *Rafty) runAsCandidate() {
	r.logState(r.getState(), true, r.getCurrentTerm())
	r.mu.Lock()
	r.electionTimerEnabled.Store(true)
	r.electionTimer = time.NewTimer(r.randomElectionTimeout(false))
	r.mu.Unlock()

	for r.getState() == Candidate {
		select {
		case <-r.quitCtx.Done():
			r.switchState(Down, true, r.getCurrentTerm())
			return

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if r.getLeader() != nil {
				return
			}
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// when vote election timer time out, start a new election campain
		// if vote election succeed
		case <-r.electionTimer.C:
			if r.electionTimerEnabled.Load() && r.startElectionCampain.Load() {
				r.startElection()
			}

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle vote response from other nodes
		// and become a leader if conditions are met
		case vote := <-r.voteResponseChan:
			r.handleVoteResponse(vote)

		// handle vote response error from other nodes
		case voteError := <-r.voteResponseErrorChan:
			r.handleVoteResponseError(voteError)

		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()
		}
	}
}

func (r *Rafty) runAsLeader() {
	r.logState(r.getState(), true, r.getCurrentTerm())

	// According to raft paper, we need to reset nextIndex and matchIndex
	// when becoming a leader
	if !r.volatileStateInitialized.Load() {
		if r.nextIndex == nil {
			r.nextIndex = make(map[string]uint64)
			r.matchIndex = make(map[string]uint64)
		}
		r.setNextAndMatchIndex(r.ID, 0, 0)
		for _, peer := range r.Peers {
			r.setNextAndMatchIndex(peer.id, 0, 0)
		}
		r.volatileStateInitialized.Store(true)
	}

	heartbeatTicker := time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for r.getState() == Leader {
		select {
		case <-r.quitCtx.Done():
			r.switchState(Down, true, r.getCurrentTerm())
			return

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if r.getLeader() != nil {
				return
			}
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle vote response from other nodes
		// and become a leader if conditions are met
		case vote := <-r.voteResponseChan:
			r.handleVoteResponse(vote)

		// handle vote response error from other nodes
		case voteError := <-r.voteResponseErrorChan:
			r.handleVoteResponseError(voteError)

		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()

		// send heartbeats to other nodes when ticker time out
		case <-heartbeatTicker.C:
			r.appendEntries(true, make(chan appendEntriesResponse, 1), false, false, -1)

		// this chan is used by clients to apply commands on the leader
		case data := <-r.triggerAppendEntriesChan:
			heartbeatTicker.Stop()
			r.log = append(r.log, &raftypb.LogEntry{TimeStamp: uint32(time.Now().Unix()), Term: r.getCurrentTerm(), Command: data.command})
			entryIndex := len(r.log) - 1
			r.appendEntries(false, data.responseChan, true, false, entryIndex)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)

		// commands sent by clients to Follower nodes will be forwarded to the leader
		// to later apply commands
		case command := <-r.rpcForwardCommandToLeaderRequestChanReader:
			heartbeatTicker.Stop()
			r.log = append(r.log, &raftypb.LogEntry{TimeStamp: uint32(time.Now().Unix()), Term: r.getCurrentTerm(), Command: command.GetCommand()})
			entryIndex := len(r.log) - 1
			r.appendEntries(false, make(chan appendEntriesResponse, 1), false, true, entryIndex)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
		}
	}
}
