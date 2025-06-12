package rafty

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// replicationRetryTimeout is the maximum amount of time to wait for a replication
	// to succeed before timing out
	replicationRetryTimeout = 50 * time.Millisecond

	// replicationMaxRetry is the maximum number of retry to perform before stop retrying
	replicationMaxRetry uint64 = 3
)

var (
	// retryableStatusCodes is a mapping used to check if we should retry to
	// send append entries
	retryableStatusCodes = map[codes.Code]bool{
		codes.Unavailable: true, // etc
	}
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

	// newEntry is used every the leader received
	// a new log entry
	newEntryChan chan *onAppendEntriesRequest

	// replicationInitialized is a chan that telling the leader
	// that replication has been initialized
	replicationInitialized chan struct{}

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

	// entryIndex is the index of the entry that will be used later
	// to store it on disk
	entryIndex int

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
	// to reply the client by using rpcForwardCommandToLeaderRequestChanWritter var
	replyToForwardedCommand bool

	// rpcForwardCommandToLeaderRequestChanWritter is used by replyToForwardedCommand
	replyToForwardedCommandChan chan *raftypb.ForwardCommandToLeaderResponse

	// uuid is used only for debugging.
	// It helps to differenciate append entries requests
	uuid string

	// entries are logs to use when
	// followerReplication.catchup is set to true
	entries []*raftypb.LogEntry

	// catchup tell us if the follower is currently catching up entries.
	// It will also be used when a new leader is promoted with term > 1
	catchup bool
}

// startFollowerReplication is instanciate for every
// follower by the leader in order to replicate append entries
func (r *followerReplication) startFollowerReplication() {
	r.replicationInitialized <- struct{}{}
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
					time.AfterFunc(backoff(replicationRetryTimeout, r.failures.Load(), replicationMaxRetry), func() {
						if r.replicationStopped.Load() || r.rafty.getState() != Leader {
							return
						}
					})
				}
				r.rafty.wg.Add(1)
				r.appendEntries(entry)
			}
		//nolint staticcheck
		default:
		}
	}
}

// sendAppendEntries send append entries to the current follower
func (r *followerReplication) sendAppendEntries(client raftypb.RaftyClient, request *onAppendEntriesRequest) (response *raftypb.AppendEntryResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.rafty.randomRPCTimeout(true))
	defer cancel()
	response, err = client.SendAppendEntriesRequest(
		ctx,
		&raftypb.AppendEntryRequest{
			LeaderAddress:     r.rafty.Address.String(),
			LeaderID:          r.rafty.id,
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
	defer r.rafty.wg.Done()
	// if the node need to catchup and an append hearbeat is sent, skip
	if r.catchup.Load() && request.heartbeat {
		r.rafty.Logger.Trace().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", request.term)).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("heartbeat", fmt.Sprintf("%t", request.heartbeat)).
			Str("catchup", fmt.Sprintf("%t", r.catchup.Load())).
			Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
			Str("commitIndex", fmt.Sprintf("%d", request.commitIndex)).
			Str("totalLogs", fmt.Sprintf("%d", request.totalLogs)).
			Msgf("Catching up")
		return
	}
	client := r.rafty.connectionManager.getClient(r.address.String(), r.ID)
	if client != nil && r.rafty.getState() == Leader {
		r.rafty.Logger.Trace().
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
			Msgf("Send append entries")

		for retry := range replicationMaxRetry {
			if r.replicationStopped.Load() || r.rafty.getState() != Leader {
				return
			}

			nextAttemptIn := backoff(replicationRetryTimeout, retry+1, replicationMaxRetry)
			if retry > 0 {
				time.AfterFunc(nextAttemptIn, func() {
					if r.replicationStopped.Load() || r.rafty.getState() != Leader {
						return
					}
				})
			}

			response, err := r.sendAppendEntries(client, request)
			if err != nil && r.rafty.getState() == Leader {
				r.failures.Add(1)

				var doNotRetry bool
				if !retryableStatusCodes[status.Code(err)] {
					// The RPC was successful or errored in a non-retryable way;
					// do not retry.
					doNotRetry = true
				}
				r.rafty.Logger.Error().Err(err).
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Str("term", fmt.Sprintf("%d", request.term)).
					Str("peerAddress", r.address.String()).
					Str("peerId", r.ID).
					Str("retryable", fmt.Sprintf("%t", doNotRetry)).
					Str("attempt", fmt.Sprintf("%d", retry)).
					Str("nextAttemptIn", nextAttemptIn.String()).
					Msgf("Fail to send append entries to peer")

				if doNotRetry {
					return
				}
			} else {
				r.failures.Store(0)

				// must be keep with large cluster like 7+ nodes when leader is stepping down
				if response == nil {
					return
				}

				r.lastContactDate.Store(time.Now())
				switch {
				case response.Term > request.term:
					r.rafty.switchState(Follower, stepDown, true, response.Term)
					return

				case response.Success:
					if !r.ReadOnlyNode {
						request.majority.Add(1)
					}

					if request.majority.Load()+1 >= request.quorum {
						if !request.heartbeat {
							if request.totalLogs > 0 && !request.committed.Load() {
								if r.rafty.options.PersistDataOnDisk && !request.committed.Load() {
									if err := r.rafty.storage.data.storeWithEntryIndex(request.entryIndex); err != nil {
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
									request.replyToClientChan <- appendEntriesResponse{}
								}
								if request.replyToForwardedCommand {
									request.replyToForwardedCommandChan <- &raftypb.ForwardCommandToLeaderResponse{}
								}
							}
						}
						r.rafty.Logger.Trace().
							Str("address", r.rafty.Address.String()).
							Str("id", r.rafty.id).
							Str("state", r.rafty.getState().String()).
							Str("term", fmt.Sprintf("%d", request.term)).
							Str("index", fmt.Sprintf("%d", request.entryIndex)).
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

					// if log not found and no ongoing catchup
					case response.LogNotFound && !r.catchup.Load() && !r.replicationStopped.Load():
						r.catchup.Store(true)
						r.rafty.wg.Add(1)
						go func() {
							defer r.rafty.wg.Done()
							r.sendCatchupAppendEntries(client, request, response)
						}()

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
}

// sendCatchupAppendEntries allow leader to send entries to the follower
// in order to catchup leader state
func (r *followerReplication) sendCatchupAppendEntries(client raftypb.RaftyClient, oldRequest *onAppendEntriesRequest, oldResponse *raftypb.AppendEntryResponse) {
	defer r.catchup.Store(false)
	logsResponse := r.rafty.logs.fromLastLogParameters(
		oldResponse.LastLogIndex,
		oldResponse.LastLogTerm,
		r.address.String(),
		r.ID,
	)
	if logsResponse.err != nil {
		r.rafty.Logger.Error().Err(logsResponse.err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("triedLastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
			Str("triedLastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
			Msgf("Failed to get catchup entries")
		return
	}

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
		Msg("Prepare catchup append entries request")
	request := &onAppendEntriesRequest{
		totalFollowers: 1,
		quorum:         1,
		term:           oldRequest.term,
		heartbeat:      false,
		prevLogIndex:   oldRequest.prevLogIndex,
		prevLogTerm:    oldRequest.prevLogTerm,
		totalLogs:      uint64(logsResponse.total),
		uuid:           uuid.NewString(),
		commitIndex:    r.rafty.commitIndex.Load(),
		entries:        logsResponse.logs,
		catchup:        true,
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
			Msgf("Fail to send catchup append entries to peer")
		return
	}

	r.failures.Store(0)

	// sometimes, during unit testing, response is nil
	// so let's avoid failure
	if response == nil {
		return
	}

	r.lastContactDate.Store(time.Now())
	if response.Success {
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
			Msgf("Follower nextIndex / matchIndex has been updated with catchup entries")
	}
}
