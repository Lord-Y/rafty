package rafty

import (
	"context"
	"fmt"
	"slices"

	"github.com/Lord-Y/rafty/grpcrequests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
)

// appendEntries permits to send append entries to followers
func (r *Rafty) appendEntries() {
	currentTerm := r.getCurrentTerm()
	commitIndex := r.getCommitIndex()
	myAddress, myId := r.getMyAddress()
	state := r.getState()
	r.mu.Lock()
	peers := r.Peers
	totalLogs := len(r.log) - 1
	r.mu.Unlock()

	for _, peer := range peers {
		var (
			prevLogTerm uint64
			entries     []*grpcrequests.LogEntry
		)
		nextIndex := r.getNextIndex(peer.id)
		prevLogIndex := nextIndex - 1

		if totalLogs > 0 {
			prevLogTerm = r.log[prevLogIndex].Term
			if uint64(totalLogs) >= nextIndex {
				entries = r.log[nextIndex:]
			}
			if len(entries) > int(maxAppendEntries) {
				entries = entries[:maxAppendEntries]
			}
		}
		totalEntries := len(entries)
		if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) && r.getState() == Leader {
			if !r.healthyPeer(peer) {
				return
			}
			go func() {
				if totalEntries == 0 {
					r.Logger.Trace().Msgf("Me %s / %s with state %s and term %d send append entries heartbeats to %s / %s", myAddress, myId, state.String(), currentTerm, peer.address.String(), peer.id)
				} else {
					r.Logger.Trace().Msgf("Me %s / %s with state %s and term %d send %d append entries to %s / %s ", myAddress, myId, state.String(), currentTerm, totalEntries, peer.address.String(), peer.id)
				}

				response, err := peer.rclient.SendAppendEntriesRequest(
					context.Background(),
					&grpcrequests.AppendEntryRequest{
						LeaderID:          myId,
						LeaderAddress:     myAddress,
						Term:              currentTerm,
						PrevLogIndex:      prevLogIndex,
						PrevLogTerm:       prevLogTerm,
						Entries:           entries,
						LeaderCommitIndex: commitIndex,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to append entries to peers %s / %s", peer.address.String(), peer.id)
					return
				}

				if response.GetTerm() > currentTerm {
					r.Logger.Debug().Msgf("Me %s / %s with state %s has lower term %d < %d than %s / %s for append entries", myAddress, myId, state.String(), currentTerm, response.GetTerm(), peer.address.String(), peer.id)
					r.setCurrentTerm(response.GetTerm())
					r.switchState(Follower, true, response.GetTerm())
					return
				}

				if r.getState() == Leader && response.GetTerm() == currentTerm {
					if response.GetSuccess() {
						if totalLogs > 0 {
							r.setNextAndMatchIndex(peer.id, max(prevLogIndex+uint64(totalEntries)+1, 1), nextIndex-1)
						} else {
							r.setNextAndMatchIndex(peer.id, 1, 0)
						}
						nextIndex, matchIndex := r.getNextAndMatchIndex(peer.id)

						r.Logger.Debug().Msgf("Me %s / %s with state %s and term %d successfully append entries of %s / %s with nextIndex: %d / matchIndex: %d", myAddress, myId, state.String(), currentTerm, peer.address.String(), peer.id, nextIndex, matchIndex)
					} else {
						nextIndex := r.getNextIndex(peer.id)

						r.Logger.Error().Err(fmt.Errorf("Fail to append entries")).Msgf("Me %s / %s with state %s and term %d failed to append entries of %s / %s because it rejected it, nextIndex: %d", myAddress, myId, state.String(), currentTerm, peer.address.String(), peer.id, nextIndex)
					}
				}
			}()
		}
	}
}
