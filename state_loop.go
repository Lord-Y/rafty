package rafty

import (
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// commonLoop handle all request that all
// nodes have in common
func (r *Rafty) commonLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.quitCtx.Done():
			return

		// handle client get leader
		case data := <-r.rpcClientGetLeaderChan:
			r.getLeaderResult(data)

		// handle ask node id
		case data := <-r.rpcAskNodeIDChan:
			r.askNodeIDResult(data)
		}
	}
}

// logsLoop handle all request related to logs
func (r *Rafty) logsLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.quitCtx.Done():
			return

		// this chan is used to read or write logs safely in memory
		case data := <-r.logs.logOperationChan:
			switch data.kind {
			case logOperationWrite:
				request := data.request.(logOperationWriteRequest)
				totalLogs := len(r.logs.log)
				for index, entry := range request.logs {
					entry.Index = uint64(totalLogs + index)
					r.logs.log = append(r.logs.log, entry)
				}
				totalLogs = len(r.logs.log)
				r.lastLogIndex.Store(uint64(totalLogs - 1))
				r.lastLogTerm.Store(uint64(r.logs.log[r.lastLogIndex.Load()].Term))
				data.responseChan <- logOperationResponse{
					response: logOperationWriteResponse{
						total: totalLogs,
					},
				}

			case logOperationWipeFromRange:
				request := data.request.(logOperationWipeRequest)
				response := logOperationWipeResponse{}
				totalLogs := len(r.logs.log)
				if totalLogs > int(request.from) && totalLogs > int(request.to) {
					r.logs.log = slices.Delete(r.logs.log, int(request.from), int(request.to))
					totalLogs = len(r.logs.log)
					r.lastLogIndex.Store(uint64(totalLogs - 1))
					r.lastLogTerm.Store(uint64(r.logs.log[r.lastLogIndex.Load()].Term))
					response.total = totalLogs
				} else {
					response.err = ErrIndexOutOfRange
				}
				data.responseChan <- logOperationResponse{
					response: response,
				}

			case logOperationGetFromIndex:
				request := data.request.(logOperationReadRequest)
				response := logOperationReadResponse{}
				totalLogs := len(r.logs.log)
				if totalLogs > 0 && totalLogs > int(request.index) {
					response.logs = make([]*raftypb.LogEntry, 1)
					copy(response.logs, []*raftypb.LogEntry{r.logs.log[request.index]})
					response.total = 1
				} else {
					response.err = ErrIndexOutOfRange
				}
				data.responseChan <- logOperationResponse{
					response: response,
				}

			case logOperationGetFromLastLogParameters:
				request := data.request.(logOperationReadLastLogRequest)
				totalLogs := len(r.logs.log)
				var limit uint
				response := logOperationReadLastLogResponse{}
				if request.lastLogIndex == 0 && request.lastLogTerm == 0 {
					limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), 0)
					response.logs = make([]*raftypb.LogEntry, limit)
					copy(response.logs, r.logs.log[0:limit])
				} else {
					if index := slices.IndexFunc(r.logs.log, func(p *raftypb.LogEntry) bool {
						return p.Index == request.lastLogIndex && p.Term == request.lastLogTerm
					}); index != -1 {
						limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), uint(index))
						for _, entry := range r.logs.log[index:limit] {
							if entry.Term <= request.lastLogTerm {
								response.logs = append(response.logs, entry)
							}
						}
					} else {
						// finding closest entry
						if index := slices.IndexFunc(r.logs.log, func(p *raftypb.LogEntry) bool {
							return p.Index > request.lastLogIndex
						}); index != -1 {
							limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), uint(index))
							response.logs = make([]*raftypb.LogEntry, limit)
							copy(response.logs, r.logs.log[index:limit])
						}
					}
				}
				response.total = len(response.logs)
				data.responseChan <- logOperationResponse{
					response: response,
				}

			case logOperationGetTotal:
				resp := logOperationReadResponse{total: len(r.logs.log)}
				data.responseChan <- logOperationResponse{
					response: resp,
				}

			default:
				totalLogs := len(r.logs.log)
				response := logOperationReadResponse{}
				response.logs = make([]*raftypb.LogEntry, totalLogs)
				response.total = totalLogs
				copy(response.logs, r.logs.log)
				data.responseChan <- logOperationResponse{
					response: response,
				}
			}

		//nolint staticcheck
		default:
		}
	}
}

// stateLoop handle all needs required by all kind of nodes
func (r *Rafty) stateLoop() {
	defer r.wg.Done()

	down := down{rafty: r}
	readOnly := readOnly{rafty: r}
	follower := follower{rafty: r}
	candidate := candidate{rafty: r}
	leader := leader{rafty: r}

	states := map[string]interface {
		init()
		onTimeout()
		release()
	}{
		"down":      &down,
		"readOnly":  &readOnly,
		"follower":  &follower,
		"candidate": &candidate,
		"leader":    &leader,
	}
	r.mu.Lock()
	r.timer = time.NewTicker(r.randomElectionTimeout())
	r.leaderLeaseDuration = r.heartbeatTimeout()
	r.leaderLeaseTimer = time.NewTicker(r.leaderLeaseDuration * 3)
	r.mu.Unlock()

	for {
		state := r.getState()
		states[state.String()].init()

		for r.getState() == state {
			select {
			// exiting for loop
			case <-r.quitCtx.Done():
				return

			// action to perform on electionTimeout/heartbearTimeout
			case <-r.timer.C:
				states[state.String()].onTimeout()

			// this chan will check if lease is still valid
			case <-r.leaderLeaseTimer.C:
				if r.getState() == Leader {
					leader.leasing()
				}

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
			case data, ok := <-candidate.responsePreVoteChan:
				if ok {
					candidate.handlePreVoteResponse(data)
				}

			// handle vote response from other nodes
			// and become a leader if conditions are met
			case data, ok := <-candidate.responseVoteChan:
				if ok {
					candidate.handleVoteResponse(data)
				}

			// handle append entries from the leader
			case data, ok := <-r.rpcAppendEntriesRequestChan:
				if ok {
					r.handleSendAppendEntriesRequest(data)
				}

			// this chan is used by clients to apply commands on the leader
			case data, ok := <-r.triggerAppendEntriesChan:
				if ok {
					leader.handleAppendEntriesFromClients("trigger", data)
				}

			// commands sent by clients to Follower nodes will be forwarded to the leader
			// to later apply commands
			case data, ok := <-r.rpcForwardCommandToLeaderRequestChan:
				if ok {
					leader.handleAppendEntriesFromClients("forwardCommand", data)
				}
			}
		}
		states[state.String()].release()
	}
}

func (r *Rafty) release() {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	r.connectionManager.disconnectAllPeers()
}
