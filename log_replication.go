package rafty

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
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
		// prevent excessive retry errors
		if r.failures.Load() > 0 {
			select {
			case <-r.rafty.quitCtx.Done():
				return
			case <-time.After(backoff(replicationRetryTimeout, r.failures.Load(), replicationMaxRetry)):
			}
		}

		select {
		case <-r.rafty.quitCtx.Done():
			return
		case heartbeat, ok := <-r.newEntryChan:
			if ok {
				r.appendEntries(heartbeat)
			}
		//nolint staticcheck
		default:
		}
	}
}

// sendAppendEntries send append entries to the current follower
func (r *followerReplication) sendAppendEntries(client raftypb.RaftyClient, request *onAppendEntriesRequest) (*raftypb.AppendEntryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), request.rpcTimeout)
	defer cancel()

	return client.SendAppendEntriesRequest(
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
}

// appendEntries send append entries to the current follower
func (r *followerReplication) appendEntries(heartbeat bool) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	previousLogIndex, previousLogTerm := r.rafty.getPreviousLogIndexAndTerm()
	var response GetLogsByRangeResponse
	// This is done to prevent fetching log entries if the follower is already synced.
	// That will reduce network traffic
	if r.rafty.matchIndex.Load() == 0 || !heartbeat {
		r.catchup.Store(true)
		defer r.catchup.Store(false)
		response = r.rafty.logStore.GetLogsByRange(previousLogIndex, r.nextIndex.Load(), r.rafty.options.MaxAppendEntries)
	}

	request := &onAppendEntriesRequest{
		term:                       r.rafty.currentTerm.Load(),
		heartbeat:                  heartbeat,
		catchup:                    !heartbeat,
		prevLogIndex:               previousLogIndex,
		prevLogTerm:                previousLogTerm,
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    makeProtobufLogEntries(response.Logs),
		totalLogs:                  response.Total,
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &atomic.Bool{},
	}

	if client := r.rafty.connectionManager.getClient(r.address.String()); client != nil && r.rafty.getState() == Leader {
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
				Str("failures", fmt.Sprintf("%d", r.failures.Load())).
				Msgf("Fail to send append entries to peer")
			return
		}

		r.failures.Store(0)
		r.lastContactDate.Store(time.Now())
		// must be kept as sometimes it's really nil
		if response == nil {
			return
		}
		if !heartbeat {
			r.catchup.Store(false)
		}

		switch {
		case response.Term > request.term:
			r.rafty.leadershipTransferDisabled.Store(true)
			r.rafty.switchState(Follower, stepDown, true, response.Term)
			return

		case response.Success:
			if r.matchIndex.Load() != response.LastLogIndex {
				r.nextIndex.Store(response.LastLogIndex + 1)
				r.matchIndex.Store(response.LastLogIndex)
				if !r.ReadReplica {
					select {
					case <-r.rafty.quitCtx.Done():
						return
					case <-time.After(100 * time.Millisecond):
						return
					case r.notifyLeader <- commitChanConfig{id: r.ID, matchIndex: response.LastLogIndex}:
					}
				}

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
					Msgf("Follower nextIndex / matchIndex has been updated")
			}

		default:
			switch {
			case r.nextIndex.Load() > 0:
				r.nextIndex.Store(r.nextIndex.Load() - 1)
				fallthrough

			// if log not found and no ongoing catchup
			case !request.heartbeat && response.LogNotFound && !r.catchup.Load() && !r.replicationStopped.Load():
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
	}
}

// sendCatchupAppendEntries allow leader to send entries to the follower
// in order to catchup leader state
func (r *followerReplication) sendCatchupAppendEntries(client raftypb.RaftyClient, oldRequest *onAppendEntriesRequest, oldResponse *raftypb.AppendEntryResponse) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.catchup.Store(true)
	defer r.catchup.Store(false)

	result := r.rafty.logStore.GetLogsByRange(oldResponse.LastLogIndex, oldRequest.prevLogIndex, r.rafty.options.MaxAppendEntries)
	if result.Total == 0 {
		r.rafty.Logger.Warn().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", oldRequest.term)).
			Str("prevLogIndex", fmt.Sprintf("%d", oldRequest.prevLogIndex)).
			Str("prevLogTerm", fmt.Sprintf("%d", oldRequest.prevLogTerm)).
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
		Str("resultLastLogIndex", fmt.Sprintf("%d", result.LastLogIndex)).
		Str("resultLastLogTerm", fmt.Sprintf("%d", result.LastLogTerm)).
		Str("totalLogs", fmt.Sprintf("%d", result.Total)).
		Str("peerAddress", r.address.String()).
		Str("peerId", r.ID).
		Str("peerLastLogIndex", fmt.Sprintf("%d", oldResponse.LastLogIndex)).
		Str("peerLastLogTerm", fmt.Sprintf("%d", oldResponse.LastLogTerm)).
		Str("membershipChange", fmt.Sprintf("%t", oldRequest.membershipChangeInProgress.Load())).
		Msg("Prepare catchup append entries request")

	request := &onAppendEntriesRequest{
		term:                       oldRequest.term,
		heartbeat:                  false,
		prevLogIndex:               oldRequest.prevLogIndex,
		prevLogTerm:                oldRequest.prevLogTerm,
		totalLogs:                  uint64(result.Total),
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
	if response != nil && response.Success && r.matchIndex.Load() != response.LastLogIndex {
		r.nextIndex.Store(response.LastLogIndex + 1)
		r.matchIndex.Store(response.LastLogIndex)
		oldResponse.LastLogIndex = response.LastLogIndex
		oldResponse.LastLogTerm = response.LastLogTerm
		if !r.ReadReplica {
			select {
			case <-r.rafty.quitCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				return
			case r.notifyLeader <- commitChanConfig{id: r.ID, matchIndex: response.LastLogIndex}:
			}
		}

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

// catchupNewMember is used by the leader to send catchup entries
// to the new peer.
// If error is returned, it must retry membership change process.
// If node nextIndex is great or equal to provided leader nextIndex
// the node is in sync with the leader.
// In raft paper, this is generally done in the normal raft replication process
// but we did a dedicated on only when a node is cathing up logs with the leader.
func (r *followerReplication) catchupNewMember(member Peer, request *onAppendEntriesRequest, response *raftypb.AppendEntryResponse) error {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	timeout := r.rafty.electionTimeout() * 10
	membershipTimer := time.NewTicker(timeout)
	leaderNextIndex := r.rafty.nextIndex.Load()
	leaderMatchIndex := r.rafty.matchIndex.Load()

	for round := range maxRound {
		lastContactDate := r.lastContactDate.Load()
		select {
		case <-r.rafty.quitCtx.Done():
			return ErrShutdown

		case <-membershipTimer.C:
			return ErrMembershipChangeNodeTooSlow

		default:
			if client := r.rafty.connectionManager.getClient(member.Address); client != nil && r.rafty.getState() == Leader {
				r.rafty.Logger.Debug().
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Str("term", fmt.Sprintf("%d", request.term)).
					Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
					Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
					Str("peerAddress", r.Address).
					Str("peerId", r.ID).
					Str("leaderNextIndex", fmt.Sprintf("%d", leaderNextIndex)).
					Str("leaderMatchIndex", fmt.Sprintf("%d", leaderMatchIndex)).
					Str("leaderPrevLogIndex", fmt.Sprintf("%d", request.prevLogIndex)).
					Str("leaderPrevLogTerm", fmt.Sprintf("%d", request.prevLogTerm)).
					Str("membershipChange", fmt.Sprintf("%t", request.membershipChangeInProgress.Load())).
					Str("round", fmt.Sprintf("%d", round)).
					Msgf("Begin catching up log entries for membership")

				r.sendCatchupAppendEntries(client, request, response)

				if lastContactDate != r.lastContactDate.Load() {
					membershipTimer.Reset(timeout)
				}

				if r.nextIndex.Load() >= leaderNextIndex {
					return nil
				}
			}
		}
	}
	return ErrMembershipChangeNodeTooSlow
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
