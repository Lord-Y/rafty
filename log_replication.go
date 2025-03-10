package rafty

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
)

// commandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type commandKind uint32

const (
	// commandGet command allow us to fetch data from the cluster
	commandGet commandKind = iota

	// commandSet command allow us to write data from the cluster
	commandSet
)

// command is the struct to use to interact with cluster data
type command struct {
	// kind represent the set of commands: get, set, del
	kind commandKind

	// key is the name of the key
	key string

	// value is the value associated to the key
	value string
}

// appendEntriesResponse is used to answer back to the client
// when fetching or appling log entries
type appendEntriesResponse struct {
	Data  []byte
	Error error
}

type triggerAppendEntries struct {
	command      []byte
	responseChan chan appendEntriesResponse
}

// appendEntries permits to send append entries to followers
func (r *Rafty) appendEntries(heartbeat bool, replyClientChan chan appendEntriesResponse, replyToClientChan, replyToForwardedCommand bool, entryIndex int) {
	currentTerm := r.getCurrentTerm()
	commitIndex := r.getCommitIndex()
	myNextIndex := r.getNextIndex(r.id)
	r.mu.Lock()
	peers := r.configuration.ServerMembers
	totalPeers := len(peers)
	totalLogs := len(r.log)
	r.mu.Unlock()
	var (
		majority                   atomic.Uint64
		leaderVolatileStateUpdated atomic.Bool
	)
	totalMajority := (totalPeers / 2) + 1

	for _, peer := range peers {
		var (
			prevLogTerm uint64
			entries     []*raftypb.LogEntry
		)
		nextIndex := r.getNextIndex(peer.ID)
		prevLogIndex := nextIndex - 1

		if totalLogs > 0 && int(prevLogIndex) >= 0 {
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
			go func() {
				if totalEntries == 0 {
					r.Logger.Trace().
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Msgf("Send append entries heartbeats")
				} else {
					r.Logger.Trace().
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Msgf("Send %d append entries", totalEntries)
				}

				response, err := peer.rclient.SendAppendEntriesRequest(
					context.Background(),
					&raftypb.AppendEntryRequest{
						LeaderID:          r.id,
						LeaderAddress:     r.Address.String(),
						Term:              currentTerm,
						PrevLogIndex:      prevLogIndex,
						PrevLogTerm:       prevLogTerm,
						Entries:           entries,
						LeaderCommitIndex: commitIndex,
						Heartbeat:         heartbeat,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					if r.getState() != Down {
						r.Logger.Error().Err(err).
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("state", r.getState().String()).
							Str("term", fmt.Sprintf("%d", currentTerm)).
							Str("peerAddress", peer.address.String()).
							Str("peerId", peer.ID).
							Msgf("Fail to send append entries to peer")
					}
					return
				}

				if response.Term > currentTerm {
					r.Logger.Debug().
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("term", fmt.Sprintf("%d", currentTerm)).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Str("peerTerm", fmt.Sprintf("%d", response.Term)).
						Msgf("My term is lower than peer")

					r.setCurrentTerm(response.GetTerm())
					r.switchState(Follower, true, response.GetTerm())
					return
				}

				if r.getState() == Leader && response.GetTerm() == currentTerm {
					if response.GetSuccess() {
						if !peer.ReadOnlyNode {
							majority.Add(1)
						}
						if int(majority.Load()) >= totalMajority {
							r.Logger.Trace().
								Str("address", r.Address.String()).
								Str("id", r.id).
								Str("state", r.getState().String()).
								Str("term", fmt.Sprintf("%d", currentTerm)).
								Str("peerAddress", peer.address.String()).
								Str("peerId", peer.ID).
								Str("peerTerm", fmt.Sprintf("%d", response.Term)).
								Str("heartbeat", fmt.Sprintf("%t", heartbeat)).
								Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
								Msgf("Replication majority reached")

							if totalLogs > 0 && !heartbeat {
								r.setNextAndMatchIndex(peer.ID, max(prevLogIndex+uint64(totalEntries)+1, 1), nextIndex-1)

								if !leaderVolatileStateUpdated.Load() {
									r.setNextAndMatchIndex(r.id, myNextIndex+1, myNextIndex)
									r.incrementLeaderCommitIndex()
									r.incrementLastApplied()
									leaderVolatileStateUpdated.Store(true)
								}

								leaderNextIndex, leaderMatchIndex := r.getNextAndMatchIndex(r.id)
								r.Logger.Debug().
									Str("address", r.Address.String()).
									Str("id", r.id).
									Str("state", r.getState().String()).
									Str("term", fmt.Sprintf("%d", currentTerm)).
									Str("nextIndex", fmt.Sprintf("%d", leaderNextIndex)).
									Str("matchIndex", fmt.Sprintf("%d", leaderMatchIndex)).
									Str("leaderVolatileStateUpdated", fmt.Sprintf("%t", leaderVolatileStateUpdated.Load())).
									Msgf("Leader volatile state")

								if err := r.persistData(entryIndex); err != nil {
									r.Logger.Fatal().Err(err).
										Str("address", r.Address.String()).
										Str("id", r.id).
										Str("state", r.getState().String()).
										Msgf("Fail to persist data on disk")
								}
								if replyToClientChan {
									replyClientChan <- appendEntriesResponse{}
								}
								if replyToForwardedCommand {
									r.rpcForwardCommandToLeaderRequestChanWritter <- &raftypb.ForwardCommandToLeaderResponse{}
								}

								r.Logger.Debug().
									Str("address", r.Address.String()).
									Str("id", r.id).
									Str("state", r.getState().String()).
									Str("term", fmt.Sprintf("%d", currentTerm)).
									Str("peerAddress", peer.address.String()).
									Str("peerId", peer.ID).
									Msgf("Successfully append entries to the majority of servers %d >= %d", int(majority.Load()), totalMajority)
							}
						}
						nextIndex, matchIndex := r.getNextAndMatchIndex(peer.ID)

						r.Logger.Debug().
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("state", r.getState().String()).
							Str("term", fmt.Sprintf("%d", currentTerm)).
							Str("peerAddress", peer.address.String()).
							Str("peerId", peer.ID).
							Str("peerTerm", fmt.Sprintf("%d", currentTerm)).
							Str("peerNextIndex", fmt.Sprintf("%d", nextIndex)).
							Str("peerMatchIndex", fmt.Sprintf("%d", int(matchIndex))).
							Msgf("Successfully append entries to peer")
					} else {
						nextIndex := max(nextIndex-1, 1)
						r.setNextIndex(peer.ID, nextIndex)

						r.Logger.Error().Err(fmt.Errorf("Fail to append entries")).
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("peerAddress", peer.address.String()).
							Str("peerId", peer.ID).
							Str("peerTerm", fmt.Sprintf("%d", response.Term)).
							Str("peerNextIndex", fmt.Sprintf("%d", nextIndex)).
							Msgf("Fail to send append entries to peer because it rejected it")
					}
				}
			}()
		}
	}
}

func (r *Rafty) SubmitCommand(command command) ([]byte, error) {
	cmd := r.encodeCommand(command)
	resp, err := r.submitCommand(cmd)
	return resp, err
}

func (r *Rafty) submitCommand(command []byte) ([]byte, error) {
	cmd := r.decodeCommand(command)
	switch cmd.kind {
	case commandGet:
	case commandSet:
		if r.getState() == Leader {
			responseChan := make(chan appendEntriesResponse, 1)
			r.triggerAppendEntriesChan <- triggerAppendEntries{command: command, responseChan: responseChan}

			// answer back to the client
			response := <-responseChan
			return response.Data, response.Error
		} else {
			leader := r.getLeader()
			if leader == (leaderMap{}) {
				return nil, errNoLeader
			}
			peer := r.getPeerClient(leader.id)
			if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) {
				response, err := peer.rclient.ForwardCommandToLeader(
					context.Background(),
					&raftypb.ForwardCommandToLeaderRequest{
						Command: command,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(fmt.Errorf("Fail to append entries")).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("leaderAddress", peer.address.String()).
						Str("leaderId", peer.ID).
						Msgf("Fail to forward command to leader")
					return nil, err
				}
				if response.Error == "" {
					return response.Data, nil
				}
				return response.Data, err
			}
		}
	}
	return nil, errCommandNotFound
}
