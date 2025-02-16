package rafty

import (
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *Rafty) runAsReadOnly() {
	r.logState(r.getState(), true, r.getCurrentTerm())

	for r.getState() == ReadOnly {
		select {
		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()

		// handle server get leader
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)
		}
	}
}

func (r *Rafty) runAsFollower() {
	if !r.minimumClusterSizeReach.Load() {
		return
	}
	r.logState(r.getState(), true, r.getCurrentTerm())
	r.mu.Lock()
	// the following is necessary to prevent nil pointer exception when we are follower state
	// and a leader is sending appendEntries
	r.electionTimer = time.NewTimer(r.randomElectionTimeout())
	r.mu.Unlock()

	timeout := time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond
	hearbeatTimer := time.NewTimer(timeout)
	myAddress, myId := r.getMyAddress()

	for r.getState() == Follower {
		select {
		case <-hearbeatTimer.C:
			if r.getState() == Down {
				hearbeatTimer.Stop()
				return
			}
			hearbeatTimer.Reset(timeout)

			if r.leaderLost.Load() {
				r.preVoteRequest()
				return
			}

			r.mu.Lock()
			leaderLastContactDate := r.LeaderLastContactDate
			leaderLost := false
			if leaderLastContactDate != nil && time.Since(*leaderLastContactDate) > timeout {
				r.leaderLost.Store(true)
				leaderLost = true
				if r.leader != nil {
					r.oldLeader = r.leader
					r.Logger.Info().Msgf("Me %s / %s with state %s reports that Leader %s / %s has been lost for term %d", myAddress, myId, r.getState().String(), r.oldLeader.address, r.oldLeader.id, r.getCurrentTerm())
				}
				r.leader = nil
				r.votedFor = ""
			}
			r.mu.Unlock()

			if leaderLost && r.getState() != Down {
				r.startElectionCampain.Store(false)
				hearbeatTimer.Reset(timeout)
			}

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()
			hearbeatTimer.Reset(timeout)

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if !r.leaderLost.Load() {
				return
			}
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()

		// handle server get leader
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)
		}
	}
}

func (r *Rafty) runAsCandidate() {
	r.logState(r.getState(), true, r.getCurrentTerm())
	r.mu.Lock()
	r.electionTimerEnabled.Store(true)
	r.electionTimer = time.NewTimer(r.randomElectionTimeout())
	r.mu.Unlock()

	for r.getState() == Candidate {
		select {
		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if !r.leaderLost.Load() {
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

		// handle server get leader
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)
		}
	}
}

func (r *Rafty) runAsLeader() {
	r.logState(r.getState(), true, r.getCurrentTerm())

	// According to raft paper, we need to reset nextIndex and matchIndex
	// when becoming a leader
	if !r.volatileStateInitialized.Load() {
		r.setNextAndMatchIndex(r.ID, 0, 0)
		for _, peer := range r.configuration.ServerMembers {
			r.setNextAndMatchIndex(peer.ID, 0, 0)
		}
		r.volatileStateInitialized.Store(true)
	}

	heartbeatTicker := time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for r.getState() == Leader {
		select {
		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
			if !r.leaderLost.Load() {
				return
			}
			r.handlePreVoteResponse(preVote)

		// handle pre vote response error from other nodes
		case preVoteError := <-r.preVoteResponseErrorChan:
			r.handlePreVoteResponseError(preVoteError)

		// receive and answer request vote from other nodes
		case reader := <-r.rpcSendVoteRequestChanReader:
			r.handleSendVoteRequestReader(reader)

		// handle append entries from the leader
		case entries := <-r.rpcSendAppendEntriesRequestChanReader:
			r.handleSendAppendEntriesRequestReader(entries)

		// handle client get leader
		case <-r.rpcClientGetLeaderChanReader:
			r.handleClientGetLeaderReader()

		// handle server get leader
		case reader := <-r.rpcGetLeaderChanReader:
			r.handleGetLeaderReader(reader)

		// send heartbeats to other nodes when ticker time out
		case <-heartbeatTicker.C:
			r.appendEntries(true, make(chan appendEntriesResponse, 1), false, false, -1)

		// this chan is used by clients to apply commands on the leader
		case data := <-r.triggerAppendEntriesChan:
			heartbeatTicker.Stop()
			r.mu.Lock()
			r.log = append(r.log, &raftypb.LogEntry{TimeStamp: uint32(time.Now().Unix()), Term: r.getCurrentTerm(), Command: data.command})
			entryIndex := len(r.log) - 1
			r.mu.Unlock()
			r.appendEntries(false, data.responseChan, true, false, entryIndex)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)

		// commands sent by clients to Follower nodes will be forwarded to the leader
		// to later apply commands
		case command := <-r.rpcForwardCommandToLeaderRequestChanReader:
			heartbeatTicker.Stop()
			r.mu.Lock()
			r.log = append(r.log, &raftypb.LogEntry{TimeStamp: uint32(time.Now().Unix()), Term: r.getCurrentTerm(), Command: command.GetCommand()})
			entryIndex := len(r.log) - 1
			r.mu.Unlock()
			r.appendEntries(false, make(chan appendEntriesResponse, 1), false, true, entryIndex)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
		}
	}
}
