package rafty

import (
	"fmt"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// commonLoop handle all request that all
// nodes have in common
func (r *Rafty) commonLoop() {
	r.wg.Add(1)
	defer r.wg.Done()

	for {
		select {
		// exiting for loop
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
	r.wg.Add(1)
	defer r.wg.Done()

	for {
		select {
		// exiting for loop
		case <-r.quitCtx.Done():
			r.drainLogs()
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
				if totalLogs == 1 {
					response.logs = make([]*raftypb.LogEntry, 1)
					copy(response.logs, r.logs.log)
					response.total = len(response.logs)
					response.lastLogIndex = r.logs.log[response.total-1].Index
					response.lastLogTerm = r.logs.log[response.total-1].Term
					return
				}

				if request.lastLogIndex == 0 && request.lastLogTerm == 0 {
					limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), 0)
					response.logs = make([]*raftypb.LogEntry, limit)
					copy(response.logs, r.logs.log[0:limit])
					response.total = len(response.logs)
					response.lastLogIndex = request.lastLogIndex
					response.lastLogTerm = request.lastLogTerm
					return
				}

				if index := slices.IndexFunc(r.logs.log, func(p *raftypb.LogEntry) bool {
					return p.Index == request.lastLogIndex && p.Term == request.lastLogTerm
				}); index != -1 {
					limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), uint(index))
					for _, entry := range r.logs.log[index:limit] {
						if entry.Term <= request.lastLogTerm {
							response.logs = append(response.logs, entry)
						}
					}
					response.total = len(response.logs)
					if response.total > 0 {
						response.lastLogIndex = r.logs.log[response.total-1].Index
						response.lastLogTerm = r.logs.log[response.total-1].Term
					}
					data.responseChan <- logOperationResponse{
						response: response,
					}
					return
				}
				// finding closest entry
				if index := slices.IndexFunc(r.logs.log, func(p *raftypb.LogEntry) bool {
					return p.Index > request.lastLogIndex
				}); index != -1 {
					limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.options.MaxAppendEntries), uint(index))
					r.Logger.Trace().
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", r.currentTerm.Load())).
						Str("request_lastLogTerm", fmt.Sprintf("%d", request.lastLogTerm)).
						Str("index", fmt.Sprintf("%d", index)).
						Str("limit", fmt.Sprintf("%d", limit)).
						Str("limitLastLogIndex", fmt.Sprintf("%d", r.logs.log[limit-1].Index)).
						Str("limitLastLogTerm", fmt.Sprintf("%d", r.logs.log[limit-1].Term)).
						Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
						Str("peerAddress", request.peerAddress).
						Str("peerId", request.peerId).
						Msg("Catchup append entries closest request")

					// when we find the closest entry, we need to give back leader prev log index and term
					// somehow, using make cause panic then calculation the final
					// lastLogTerm and lastLogIndex response so we use for loop instead of copy
					// response.logs = make([]*raftypb.LogEntry, limit)
					// copy(response.logs, r.logs.log[index:limit])
					response.logs = append(response.logs, r.logs.log[index:limit]...)
					response.total = len(response.logs)
					if response.total > 0 {
						response.lastLogIndex = r.logs.log[response.total-1].Index
						response.lastLogTerm = r.logs.log[response.total-1].Term
					}
					data.responseChan <- logOperationResponse{
						response: response,
					}
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
	r.mu.Lock()
	r.timer = time.NewTicker(r.randomElectionTimeout())
	r.mu.Unlock()

	for r.getState() != Down {
		switch r.getState() {
		case ReadOnly:
			r.runAsReadOnly()
		case Follower:
			r.runAsFollower()
		case Candidate:
			r.runAsCandidate()
		case Leader:
			r.runAsLeader()
		}
	}
}

// runAsReadOnly will run node as readOnly
func (r *Rafty) runAsReadOnly() {
	r.wg.Add(1)
	defer r.wg.Done()

	state := readOnly{rafty: r}
	defer state.release()
	state.init()

	for r.getState() == ReadOnly {
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
