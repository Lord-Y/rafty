package rafty

import (
	"time"

	"github.com/Lord-Y/rafty/grpcrequests"
)

func (r *Rafty) runAsFollower() {
	r.logState(r.getState(), true, r.getCurrentTerm())

	for r.getState() == Follower {
		select {
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return

		// start pre vote request
		case <-r.preVoteElectionTimer.C:
			if !r.preVoteElectionTimerEnabled.Load() {
				return
			}
			r.preVoteRequest()

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
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

	for r.getState() == Candidate {
		select {
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
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
		for _, peer := range r.Peers {
			if r.nextIndex == nil {
				r.nextIndex = make(map[string]uint64)
				r.matchIndex = make(map[string]uint64)
			}
			r.setNextAndMatchIndex(peer.id, 0, 0)
		}
		r.volatileStateInitialized.Store(true)
	}

	heartbeatTicker := time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for r.getState() == Leader {
		select {
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return

		// receive and answer pre vote requests from other nodes
		case <-r.rpcPreVoteRequestChanReader:
			r.handleSendPreVoteRequestReader()

		// handle pre vote response from other nodes
		case preVote := <-r.preVoteResponseChan:
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
			r.appendEntries(make(chan appendEntriesResponse, 1), false, false)

		// this chan is used by clients to apply commands on the leader
		case data := <-r.triggerAppendEntriesChan:
			heartbeatTicker.Stop()
			r.log = append(r.log, &grpcrequests.LogEntry{Term: r.getCurrentTerm(), Command: data.command})
			r.appendEntries(data.responseChan, true, false)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)

		// commands sent by clients to Follower nodes will be forwarded to the leader
		// to later apply commands
		case command := <-r.rpcForwardCommandToLeaderRequestChanReader:
			heartbeatTicker.Stop()
			r.log = append(r.log, &grpcrequests.LogEntry{Term: r.getCurrentTerm(), Command: command.GetCommand()})
			r.appendEntries(make(chan appendEntriesResponse, 1), false, true)
			heartbeatTicker = time.NewTicker(time.Duration(leaderHeartBeatTimeout*int(r.TimeMultiplier)) * time.Millisecond)
		}
	}
}

func (r *Rafty) checkLeaderLastContactDate() {
	hearbeatTimer := time.NewTimer(r.randomElectionTimeout(true))

	for r.getState() == Follower {
		select {
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return

		case <-hearbeatTimer.C:
			timeout := r.randomElectionTimeout(true)
			hearbeatTimer = time.NewTimer(timeout)
			r.mu.Lock()
			leaderLastContactDate := r.LeaderLastContactDate
			leaderLost := false
			if leaderLastContactDate != nil && time.Since(*leaderLastContactDate) > timeout {
				r.leaderLost.Store(true)
				leaderLost = true
				if r.leader != nil {
					r.oldLeader = r.leader
					r.Logger.Info().Msgf("Leader %s / %s has been lost for term %d", r.oldLeader.address, r.oldLeader.id, r.getCurrentTerm())
				}
				r.leader = nil
				r.votedFor = ""
			}
			r.mu.Unlock()

			if leaderLost && r.getState() != Down {
				if !r.preVoteElectionTimerEnabled.Load() {
					r.resetElectionTimer(true, false)
				}
			}
		}
	}
}
