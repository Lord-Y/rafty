package rafty

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
)

// startStopFollowerReplication is instantiate for every
// follower by the leader in order to replicate append entries.
// It will automatically stop sending append entries when quitCtx is hit.
func (r *followerReplication) startStopFollowerReplication() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	defer func() {
		r.replicationStopped.Store(true)
		// Drain newEntryChan before closing
		for {
			select {
			case <-r.newEntryChan:
				// keep draining
			default:
				close(r.newEntryChan)
				r.rafty.Logger.Trace().
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Str("peerAddress", r.address.String()).
					Str("peerId", r.ID).
					Msgf("Replication stopped")
				return
			}
		}
	}()

	for r.rafty.getState() == Leader && !r.replicationStopped.Load() {
		select {
		case <-r.rafty.quitCtx.Done():
			return

		// append entries
		case entry, ok := <-r.newEntryChan:
			if ok {
				// prevent excessive retry errors
				if r.failures.Load() > 0 {
					select {
					case <-r.rafty.quitCtx.Done():
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
			PrevLogTerm:       request.prevLogTerm,
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
	client := r.rafty.connectionManager.getClient(r.address.String())
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
						if !request.committed.Load() {
							if err := r.rafty.logStore.StoreLogs(makeLogEntry(request.entries[0])); err != nil {
								panic(err)
							}

							// update leader volatile state
							request.committed.Store(true)
							if !request.leaderVolatileStateUpdated.Load() {
								request.leaderVolatileStateUpdated.Store(true)
								r.rafty.nextIndex.Add(1)
								r.rafty.matchIndex.Add(1)
								r.rafty.commitIndex.Add(1)

								r.rafty.Logger.Trace().
									Str("address", r.rafty.Address.String()).
									Str("id", r.rafty.id).
									Str("state", r.rafty.getState().String()).
									Str("term", fmt.Sprintf("%d", response.Term)).
									Str("peerAddress", r.address.String()).
									Str("peerId", r.ID).
									Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
									Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
									Str("commitIndex", fmt.Sprintf("%d", r.rafty.commitIndex.Load())).
									Str("lastApplied", fmt.Sprintf("%d", r.rafty.lastApplied.Load())).
									Str("requestUUID", request.uuid).
									Msgf("Leader volatile state has been updated")
							}

							if request.entries[0].LogType == uint32(LogReplication) {
								if _, err := r.rafty.applyLogs(applyLogs{entries: request.entries}); err != nil {
									r.rafty.Logger.Error().Err(err).
										Str("id", r.rafty.id).
										Str("state", r.rafty.getState().String()).
										Str("term", fmt.Sprintf("%d", response.Term)).
										Msgf("Fail to apply log entries to the fsm")
								}
							}

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
						}
					}
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

	result := r.rafty.logStore.GetLogsByRange(oldResponse.LastLogIndex, oldRequest.prevLogIndex, r.rafty.options.MaxAppendEntries)
	if result.Total == 0 {
		r.rafty.Logger.Warn().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", oldRequest.term)).
			Str("lastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
			Str("lastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
			Str("totalLogs", fmt.Sprintf("%d", result.Total)).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("peerLastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
			Str("peerLastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
			Str("membershipChange", fmt.Sprintf("%t", oldRequest.membershipChangeInProgress.Load())).
			Msg("Fail to prepare catchup append entries request")
		return
	}

	if result.SendSnapshot {
		r.sendInstallSnapshot(client)
		return
	}

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("term", fmt.Sprintf("%d", oldRequest.term)).
		Str("lastLogIndex", fmt.Sprintf("%d", result.LastLogIndex)).
		Str("lastLogTerm", fmt.Sprintf("%d", result.LastLogTerm)).
		Str("totalLogs", fmt.Sprintf("%d", result.Total)).
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
		totalLogs:                  uint64(result.Total),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    makeProtobufLogEntries(result.Logs),
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
// It will automatically stop sending append entries when quitCtx is hit.
func (r *leader) startStopSingleServerReplication() {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	defer func() {
		r.singleServerReplicationStopped.Store(true)
		// Drain singleServerNewEntryChan before closing
		for {
			select {
			case <-r.singleServerNewEntryChan:
				// keep draining
			default:
				close(r.singleServerNewEntryChan)
				r.rafty.Logger.Trace().
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Msgf("Replication stopped")
				return
			}
		}
	}()

	for r.rafty.getState() == Leader && !r.singleServerReplicationStopped.Load() {
		select {
		case <-r.rafty.quitCtx.Done():
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
}

// singleServerAppendEntries manage append entries for the leader in the
// single server cluster mode
func (r *leader) singleServerAppendEntries(request *onAppendEntriesRequest) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	if err := r.rafty.logStore.StoreLogs(makeLogEntry(request.entries[0])); err != nil {
		panic(err)
	}

	r.rafty.nextIndex.Add(1)
	r.rafty.matchIndex.Add(1)
	r.rafty.commitIndex.Add(1)

	r.rafty.Logger.Trace().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("term", fmt.Sprintf("%d", request.term)).
		Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
		Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
		Str("commitIndex", fmt.Sprintf("%d", r.rafty.commitIndex.Load())).
		Str("lastApplied", fmt.Sprintf("%d", r.rafty.lastApplied.Load())).
		Str("requestUUID", request.uuid).
		Msgf("Leader volatile state has been updated")

	if request.entries[0].LogType == uint32(LogReplication) {
		if _, err := r.rafty.applyLogs(applyLogs{entries: request.entries}); err != nil {
			r.rafty.Logger.Error().Err(err).
				Str("id", r.rafty.id).
				Str("state", r.rafty.getState().String()).
				Str("term", fmt.Sprintf("%d", request.term)).
				Msgf("Fail to apply log entries to the fsm")
		}
	}
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

// sendAppendEntries send append entries to the current follower
func (r *followerReplication) sendInstallSnapshot(client raftypb.RaftyClient) {
	if r.sendSnapshotInProgress.Load() {
		return
	}

	r.sendSnapshotInProgress.Store(true)
	defer r.sendSnapshotInProgress.Store(false)

	snapshots := r.rafty.snapshot.List()
	if len(snapshots) == 0 {
		r.rafty.Logger.Warn().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Msgf("No snapshot found to send")
		return
	}

	metadata := snapshots[0]
	_, file, err := r.rafty.snapshot.PrepareSnapshotReader(metadata.SnapshotName)
	if err != nil {
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("snapshotName", metadata.SnapshotName).
			Msgf("Snapshot not found")
		return
	}
	defer func() {
		_ = file.Close()
	}()

	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, file)
	if err != nil {
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("snapshotName", metadata.SnapshotName).
			Msgf("Fail to read snapshot")
		return
	}

	currentTerm := r.rafty.currentTerm.Load()
	response, err := client.SendInstallSnapshotRequest(
		context.Background(),
		&raftypb.InstallSnapshotRequest{
			LeaderAddress:          r.rafty.Address.String(),
			LeaderId:               r.rafty.id,
			LastIncludedIndex:      metadata.LastIncludedIndex,
			LastIncludedTerm:       metadata.LastIncludedTerm,
			LastAppliedConfigIndex: metadata.LastAppliedConfigIndex,
			LastAppliedConfigTerm:  metadata.LastAppliedConfigTerm,
			Configuration:          EncodePeers(metadata.Configuration.ServerMembers),
			Data:                   buffer.Bytes(),
			Size:                   uint64(metadata.Size),
			CurrentTerm:            currentTerm,
		},
	)

	if err != nil {
		r.failures.Add(1)
		r.rafty.Logger.Error().Err(err).
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("peerId", r.ID).
			Str("snapshotName", metadata.SnapshotName).
			Msgf("Fail to perform install snapshot request")
		return
	}

	r.lastContactDate.Store(time.Now())
	if response.Term > currentTerm {
		r.rafty.leadershipTransferDisabled.Store(true)
		r.rafty.currentTerm.Store(response.Term)
		r.rafty.switchState(Follower, stepDown, true, response.Term)
	}

	if response.Success {
		r.nextIndex.Add(metadata.LastIncludedIndex + 1)
		r.matchIndex.Add(metadata.LastIncludedIndex)
		r.failures.Store(0)

		r.rafty.Logger.Info().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("peerAddress", r.address.String()).
			Str("snapshotName", metadata.SnapshotName).
			Str("peerId", r.ID).
			Str("peerTerm", fmt.Sprintf("%d", response.Term)).
			Str("peerNextIndex", fmt.Sprintf("%d", metadata.LastIncludedIndex+1)).
			Str("peerMatchIndex", fmt.Sprintf("%d", metadata.LastIncludedIndex)).
			Msgf("Install snapshot successful")
		return
	}

	r.failures.Add(1)
	r.rafty.Logger.Error().Err(err).
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("snapshotName", metadata.SnapshotName).
		Str("peerAddress", r.address.String()).
		Str("peerId", r.ID).
		Str("snapshotLastIncludedIndex", fmt.Sprintf("%d", metadata.LastIncludedIndex)).
		Str("snapshotLastIncludedTerm", fmt.Sprintf("%d", metadata.LastIncludedTerm)).
		Str("snapshotLastAppliedConfigIndex", fmt.Sprintf("%d", metadata.LastAppliedConfigIndex)).
		Str("snapshotLastAppliedConfigTerm", fmt.Sprintf("%d", metadata.LastAppliedConfigTerm)).
		Str("peerTerm", fmt.Sprintf("%d", response.Term)).
		Msgf("Fail install snapshot to peer")
}
