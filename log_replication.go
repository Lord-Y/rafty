package rafty

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
)

const (
	// replicationRetryTimeout is the maximum amount of time to wait for a replication
	// to succeed before timing out
	replicationRetryTimeout = 50 * time.Millisecond

	// replicationMaxRetry is the maximum number of retry to perform before stop retrying
	replicationMaxRetry uint64 = 3

	// maxRound is used during membership request. When reached, it means that the new member
	// is to slow and must retry membership process in order before being promoted
	maxRound uint64 = 10
)

// appendEntriesResponse is used to answer back to the client
// when fetching or appling log entries
type appendEntriesResponse struct {
	Data  []byte
	Error error
}

// triggerAppendEntries will be used by triggerAppendEntriesChan
type triggerAppendEntries struct {
	command      []byte
	responseChan chan appendEntriesResponse
}

// followerReplication hold all requirements that allow the leader to replicate
// its logs to the current follower
type followerReplication struct {
	// peer holds peer informations
	peer

	// rafty holds rafty config
	rafty *Rafty

	// newEntry is used by the leader every times it received
	// a new log entry
	newEntryChan chan *onAppendEntriesRequest

	// replicationStopChan is used by the leader
	// in order stop ongoing append entries replication
	// or because the leader is stepping down as follower
	// or the leader is shutting down
	replicationStopChan chan struct{}

	// replicationStopped is only a helper to indicate if a replicationStopChan is closed
	replicationStopped atomic.Bool

	// nextIndex is the next log entry to send to that follower
	// initialized to leader last log index + 1
	nextIndex atomic.Uint64

	// matchIndex is the index of the highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotically
	matchIndex atomic.Uint64

	// failures is a counter of RPC call failed
	// It will be used to apply backoff
	failures atomic.Uint64

	// catchup tell us if the follower is currently catching up entries
	catchup atomic.Bool

	// lastContactDate is the last date we heard the follower
	lastContactDate atomic.Value
}

// onAppendEntriesRequest hold all requirements that allow the leader
// to replicate entries
type onAppendEntriesRequest struct {
	// majority represent how many nodes the append entries request
	// has been successful.
	// It will then be used with totalMajority var to determine whether
	// logs must be committed on disk.
	// It only incremented by voters
	majority atomic.Uint64

	// quorum is the minimum number to be reached before commit logs
	// to disk
	quorum uint64

	// totalFollowers hold the total number of nodes for which the leader has sent
	// the append entries request
	totalFollowers uint64

	// totalLogs is the total logs the leader currently have
	totalLogs uint64

	// term is the current term of the leader
	term uint64

	// hearbeat stand here if the leader have to send hearbeat append entries
	heartbeat bool

	// commitIndex is the leader commit index
	commitIndex uint64

	// prevLogIndex is the leader commit index
	prevLogIndex uint64

	// prevLogTerm is the leader commit index
	prevLogTerm uint64

	// committed tell us if the log has already been committed on disk
	committed atomic.Bool

	// leaderVolatileStateUpdated tell us if the leader volatile state has already been updated
	leaderVolatileStateUpdated atomic.Bool

	// replyToClientChan is a boolean that allow the leader
	// to reply the client by using replyClientChan var
	replyToClient bool

	// replyClientChan is used by replyToClientChan
	replyToClientChan chan appendEntriesResponse

	// replyToForwardedCommand is a boolean that allow the leader
	// to reply the client by using replyToForwardedCommandChan var
	replyToForwardedCommand bool

	// replyToForwardedCommandChan is used by replyToForwardedCommand
	replyToForwardedCommandChan chan<- RPCResponse

	// uuid is used only for debugging.
	// It helps to differenciate append entries requests
	uuid string

	// entries are logs to use when
	// followerReplication.catchup is set to true
	entries []*raftypb.LogEntry

	// catchup tell us if the follower is currently catching up entries.
	// It will also be used when a new leader is promoted with term > 1
	catchup bool

	// rpcTimeout is the timeout to use when sending append entries
	rpcTimeout time.Duration

	// membershipChangeInProgress is set to true when membership change is ongoing
	membershipChangeInProgress *atomic.Bool

	// membershipChangeCommitted is set to true when log config has been replicated
	// to the majority of servers
	membershipChangeCommitted atomic.Bool

	// membershipChangeID is the id of the member to take action on.
	// When filled membershipChangeCommitted can be updated
	membershipChangeID string
}

// startStopFollowerReplication is instantiate for every
// follower by the leader in order to replicate append entries.
// It will automatically stop to send append entries when quitCtx or
// replicationStopChan chans are hit.
func (r *followerReplication) startStopFollowerReplication() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	for r.rafty.getState() == Leader {
		select {
		case <-r.rafty.quitCtx.Done():
			return

		case <-r.replicationStopChan:
			return

		// append entries
		case entry, ok := <-r.newEntryChan:
			if ok {
				// prevent excessive retry errors
				if r.failures.Load() > 0 {
					select {
					case <-r.rafty.quitCtx.Done():
						return

					case <-r.replicationStopChan:
						return

					case <-time.After(backoff(replicationRetryTimeout, r.failures.Load(), replicationMaxRetry)):

					//nolint staticcheck
					default:
					}
				}
				r.appendEntries(entry)
			}
		//nolint staticcheck
		default:
		}
	}

	r.replicationStopped.Store(true)
	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("peerAddress", r.address.String()).
		Str("peerId", r.ID).
		Msgf("Replication stopped")

	time.Sleep(100 * time.Millisecond)
	// draining remaining calls
	for {
		select {
		case <-r.newEntryChan:
		default:
			close(r.replicationStopChan)
			close(r.newEntryChan)
			r.newEntryChan = nil
			return
		}
	}
}

// sendAppendEntries send append entries to the current follower
func (r *followerReplication) sendAppendEntries(client raftypb.RaftyClient, request *onAppendEntriesRequest) (response *raftypb.AppendEntryResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), request.rpcTimeout)
	defer cancel()
	response, err = client.SendAppendEntriesRequest(
		ctx,
		&raftypb.AppendEntryRequest{
			LeaderAddress:     r.rafty.Address.String(),
			LeaderId:          r.rafty.id,
			Term:              request.term,
			PrevLogIndex:      request.prevLogIndex,
			PrevLogTerm:       int64(request.prevLogTerm),
			Entries:           request.entries,
			LeaderCommitIndex: request.commitIndex,
			Heartbeat:         request.heartbeat,
			Catchup:           request.catchup,
		},
	)
	return
}

// appendEntries send append entries to the current follower
func (r *followerReplication) appendEntries(request *onAppendEntriesRequest) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	client := r.rafty.connectionManager.getClient(r.address.String(), r.ID)
	if client != nil && r.rafty.getState() == Leader {
		response, err := r.sendAppendEntries(client, request)
		if err != nil && r.rafty.getState() == Leader {
			r.failures.Add(1)

			r.rafty.Logger.Error().Err(err).
				Str("address", r.rafty.Address.String()).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("term", fmt.Sprintf("%d", request.term)).
				Str("peerAddress", r.address.String()).
				Str("peerId", r.ID).
				Str("heartbeat", fmt.Sprintf("%t", request.heartbeat)).
				Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
				Str("commitIndex", fmt.Sprintf("%d", request.commitIndex)).
				Str("totalLogs", fmt.Sprintf("%d", request.totalLogs)).
				Str("rpcTimeout", request.rpcTimeout.String()).
				Msgf("Fail to send append entries to peer")
		} else {
			r.failures.Store(0)

			// must be kept as sometimes it's really nil
			if response == nil {
				return
			}

			r.lastContactDate.Store(time.Now())
			switch {
			case response.Term > request.term:
				r.rafty.leadershipTransferDisabled.Store(true)
				r.rafty.switchState(Follower, stepDown, true, response.Term)
				return

			case response.Success:
				if !r.ReadReplica {
					request.majority.Add(1)
				}

				if request.majority.Load()+1 >= request.quorum {
					if !request.heartbeat {
						if request.totalLogs > 0 && !request.committed.Load() {
							if r.rafty.options.PersistDataOnDisk && !request.committed.Load() {
								if err := r.rafty.storage.data.store(request.entries[0]); err != nil {
									r.rafty.Logger.Fatal().Err(err).Msg("Fail to persist data on disk")
								}
							}

							// update leader volatile state
							request.committed.Store(true)
							if !request.leaderVolatileStateUpdated.Load() {
								request.leaderVolatileStateUpdated.Store(true)
								r.rafty.nextIndex.Add(1)
								r.rafty.matchIndex.Add(1)
								r.rafty.lastApplied.Add(1)
								r.rafty.commitIndex.Add(1)

								r.rafty.Logger.Trace().
									Str("address", r.rafty.Address.String()).
									Str("id", r.rafty.id).
									Str("state", r.rafty.getState().String()).
									Str("term", fmt.Sprintf("%d", response.Term)).
									Str("peerAddress", r.address.String()).
									Str("peerId", r.ID).
									Str("totalLogs", fmt.Sprintf("%d", request.totalLogs)).
									Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
									Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
									Str("commitIndex", fmt.Sprintf("%d", r.rafty.commitIndex.Load())).
									Str("lastApplied", fmt.Sprintf("%d", r.rafty.lastApplied.Load())).
									Str("requestUUID", request.uuid).
									Msgf("Leader volatile state has been updated")
							}
						}

						if request.committed.Load() && !r.replicationStopped.Load() {
							r.nextIndex.Add(1)
							r.matchIndex.Add(1)

							r.rafty.Logger.Trace().
								Str("address", r.rafty.Address.String()).
								Str("id", r.rafty.id).
								Str("state", r.rafty.getState().String()).
								Str("term", fmt.Sprintf("%d", response.Term)).
								Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
								Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
								Str("peerAddress", r.address.String()).
								Str("peerId", r.ID).
								Str("peerNextIndex", fmt.Sprintf("%d", r.nextIndex.Load())).
								Str("peerMatchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
								Str("requestUUID", request.uuid).
								Msgf("Follower nextIndex / matchIndex has been updated")

							// TODO: later on, we need prevent duplicate sent of data
							//
							// reply to clients if necessary
							if request.replyToClient {
								select {
								case <-r.rafty.quitCtx.Done():
									return
								case <-time.After(500 * time.Millisecond):
									return
								case request.replyToClientChan <- appendEntriesResponse{}:
								}
							}

							if request.replyToForwardedCommand {
								response := &raftypb.ForwardCommandToLeaderResponse{
									LeaderId:      r.rafty.id,
									LeaderAddress: r.rafty.Address.String(),
								}
								rpcResponse := RPCResponse{
									Response: response,
								}
								select {
								case <-r.rafty.quitCtx.Done():
									return
								case <-time.After(500 * time.Millisecond):
									return
								case request.replyToForwardedCommandChan <- rpcResponse:
								}
							}

							if request.membershipChangeInProgress.Load() && request.membershipChangeID != "" {
								request.membershipChangeCommitted.Store(true)
							}
						}
					}
					r.rafty.Logger.Trace().
						Str("address", r.rafty.Address.String()).
						Str("id", r.rafty.id).
						Str("state", r.rafty.getState().String()).
						Str("term", fmt.Sprintf("%d", request.term)).
						Str("totalLogs", fmt.Sprintf("%d", request.totalLogs)).
						Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
						Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
						Str("peerAddress", r.address.String()).
						Str("peerId", r.ID).
						Str("peerNextIndex", fmt.Sprintf("%d", r.nextIndex.Load())).
						Str("peerMatchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
						Str("heartbeat", fmt.Sprintf("%t", request.heartbeat)).
						Msgf("Successfully append entries to the majority of servers %d >= %d", request.majority.Load()+1, request.quorum)
				}

			default:
				switch {
				case r.nextIndex.Load() > 0:
					r.nextIndex.Store(r.nextIndex.Load() - 1)
					fallthrough

				// if log not found and no ongoing catchup
				case response.LogNotFound && !r.catchup.Load() && !r.replicationStopped.Load():
					r.catchup.Store(true)
					go r.sendCatchupAppendEntries(client, request, response)

				default:
					r.rafty.Logger.Error().Err(fmt.Errorf("fail to append entries to peer")).
						Str("address", r.rafty.Address.String()).
						Str("id", r.rafty.id).
						Str("state", r.rafty.getState().String()).
						Str("term", fmt.Sprintf("%d", request.term)).
						Str("peerAddress", r.address.String()).
						Str("peerId", r.ID).
						Str("peerNextIndex", fmt.Sprintf("%d", r.nextIndex.Load())).
						Str("peerMatchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
						Msgf("Failed to append entries because peer rejected it")
				}
			}
			return
		}
	}
}

// sendCatchupAppendEntries allow leader to send entries to the follower
// in order to catchup leader state
func (r *followerReplication) sendCatchupAppendEntries(client raftypb.RaftyClient, oldRequest *onAppendEntriesRequest, oldResponse *raftypb.AppendEntryResponse) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	defer r.catchup.Store(false)

	logsResponse := r.rafty.logs.fromLastLogParameters(
		oldResponse.LastLogIndex,
		oldResponse.LastLogTerm,
		r.address.String(),
		r.ID,
	)

	if logsResponse.total == 0 {
		r.rafty.Logger.Warn().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", oldRequest.term)).
			Str("lastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
			Str("lastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
			Str("totalLogs", fmt.Sprintf("%d", logsResponse.total)).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("peerLastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
			Str("peerLastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
			Str("membershipChange", fmt.Sprintf("%t", oldRequest.membershipChangeInProgress.Load())).
			Msg("Fail to prepare catchup append entries request")
		return
	}

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("term", fmt.Sprintf("%d", oldRequest.term)).
		Str("lastLogIndex", fmt.Sprintf("%d", logsResponse.lastLogIndex)).
		Str("lastLogTerm", fmt.Sprintf("%d", logsResponse.lastLogTerm)).
		Str("totalLogs", fmt.Sprintf("%d", logsResponse.total)).
		Str("peerAddress", r.address.String()).
		Str("peerId", r.ID).
		Str("peerLastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
		Str("peerLastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
		Str("membershipChange", fmt.Sprintf("%t", oldRequest.membershipChangeInProgress.Load())).
		Msg("Prepare catchup append entries request")

	request := &onAppendEntriesRequest{
		totalFollowers:             1,
		quorum:                     1,
		term:                       oldRequest.term,
		heartbeat:                  false,
		prevLogIndex:               oldRequest.prevLogIndex,
		prevLogTerm:                oldRequest.prevLogTerm,
		totalLogs:                  uint64(logsResponse.total),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    logsResponse.logs,
		catchup:                    true,
		rpcTimeout:                 oldRequest.rpcTimeout,
		membershipChangeInProgress: oldRequest.membershipChangeInProgress,
		membershipChangeID:         oldRequest.membershipChangeID,
	}

	response, err := r.sendAppendEntries(client, request)
	if err != nil && r.rafty.getState() == Leader {
		r.failures.Add(1)
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", request.term)).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("membershipChange", fmt.Sprintf("%t", request.membershipChangeInProgress.Load())).
			Msgf("Fail to send catchup append entries to peer")
		return
	}

	r.failures.Store(0)
	r.lastContactDate.Store(time.Now())
	if response != nil && response.Success {
		r.nextIndex.Add(request.totalLogs)
		r.matchIndex.Add(request.totalLogs - 1)

		r.rafty.Logger.Trace().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", response.Term)).
			Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
			Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("peerNextIndex", fmt.Sprintf("%d", r.nextIndex.Load())).
			Str("peerMatchIndex", fmt.Sprintf("%d", r.matchIndex.Load())).
			Str("membershipChange", fmt.Sprintf("%t", request.membershipChangeInProgress.Load())).
			Msgf("Follower nextIndex / matchIndex has been updated with catchup entries")
	}
}

// startStopSingleServerReplication is instanciate for single server cluster
// by the leader in order to replicate append entries.
// It will automatically stop when singleServerReplicationStopChan chan is hit.
func (r *leader) startStopSingleServerReplication() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	for r.rafty.getState() == Leader {
		select {
		case <-r.singleServerReplicationStopChan:
			return

		// append entries
		case entry, ok := <-r.singleServerNewEntryChan:
			if ok {
				r.singleServerAppendEntries(entry)
			}
		//nolint staticcheck
		default:
		}
	}

	r.singleServerReplicationStopped.Store(true)
	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Msgf("Replication stopped")

	time.Sleep(100 * time.Millisecond)
	// draining remaining calls
	for {
		select {
		case <-r.singleServerNewEntryChan:
		default:
			close(r.singleServerReplicationStopChan)
			close(r.singleServerNewEntryChan)
			r.singleServerNewEntryChan = nil
			return
		}
	}
}

// singleServerAppendEntries manage append entries for the leader in the
// single server cluster mode
func (r *leader) singleServerAppendEntries(request *onAppendEntriesRequest) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if r.rafty.options.PersistDataOnDisk {
		if err := r.rafty.storage.data.store(request.entries[0]); err != nil {
			r.rafty.Logger.Fatal().Err(err).Msg("Fail to persist data on disk")
		}
	}

	r.rafty.nextIndex.Add(1)
	r.rafty.matchIndex.Add(1)
	r.rafty.lastApplied.Add(1)
	r.rafty.commitIndex.Add(1)

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("term", fmt.Sprintf("%d", request.term)).
		Str("totalLogs", fmt.Sprintf("%d", request.totalLogs)).
		Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
		Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
		Str("commitIndex", fmt.Sprintf("%d", r.rafty.commitIndex.Load())).
		Str("lastApplied", fmt.Sprintf("%d", r.rafty.lastApplied.Load())).
		Str("requestUUID", request.uuid).
		Msgf("Leader volatile state has been updated")

	if request.replyToClient {
		select {
		case <-r.rafty.quitCtx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			return
		case request.replyToClientChan <- appendEntriesResponse{}:
		}
	}
}
