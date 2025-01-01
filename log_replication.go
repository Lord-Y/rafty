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
func (r *Rafty) appendEntries(heartbeat bool, clientChan chan appendEntriesResponse, replyToClient, replyToClientGRPC bool, entryIndex int) {
	currentTerm := r.getCurrentTerm()
	commitIndex := r.getCommitIndex()
	myAddress, myId := r.getMyAddress()
	state := r.getState()
	myNextIndex := r.getNextIndex(r.ID)
	r.murw.Lock()
	peers := r.Peers
	totalPeers := len(peers)
	totalLogs := len(r.log)
	r.murw.Unlock()
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
		nextIndex := r.getNextIndex(peer.id)
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
					&raftypb.AppendEntryRequest{
						LeaderID:          myId,
						LeaderAddress:     myAddress,
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
						majority.Add(1)
						if int(majority.Load()) >= totalMajority {
							r.Logger.Trace().Msgf("Me %s / %s with state %s and term %d  reports that majority replication reached %t totalLogs %d heartbeat %t", myAddress, myId, state.String(), currentTerm, int(majority.Load()) >= totalMajority, totalLogs, heartbeat)
							if totalLogs > 0 && !heartbeat {
								r.setNextAndMatchIndex(peer.id, max(prevLogIndex+uint64(totalEntries)+1, 1), nextIndex-1)

								if !leaderVolatileStateUpdated.Load() {
									r.setNextAndMatchIndex(r.ID, myNextIndex+1, myNextIndex)
									r.incrementLeaderCommitIndex()
									r.incrementLastApplied()
									leaderVolatileStateUpdated.Store(true)
								}

								leaderNextIndex, leaderMatchIndex := r.getNextAndMatchIndex(r.ID)
								r.Logger.Debug().Msgf("Me %s / %s with state %s and term %d nextIndex: %d / matchIndex: %d leaderVolatileStateUpdated %t", myAddress, myId, state.String(), currentTerm, leaderNextIndex, leaderMatchIndex, leaderVolatileStateUpdated.Load())

								if replyToClient {
									clientChan <- appendEntriesResponse{}
								}
								if replyToClientGRPC {
									r.rpcForwardCommandToLeaderRequestChanWritter <- &raftypb.ForwardCommandToLeaderResponse{}
								}
								if r.PersistDataOnDisk {
									err = r.persistData(entryIndex)
									if err != nil {
										r.Logger.Fatal().Err(err).Msg("Fail to persist data on storage")
									}
								}
								r.Logger.Debug().Msgf("Me %s / %s with state %s and term %d successfully append entries to the majority of servers %d >= %d", myAddress, myId, state.String(), currentTerm, int(majority.Load()), totalMajority)
							}
						}
						nextIndex, matchIndex := r.getNextAndMatchIndex(peer.id)

						r.Logger.Debug().Msgf("Me %s / %s with state %s and term %d successfully append entries of %s / %s with nextIndex: %d / matchIndex: %d", myAddress, myId, state.String(), currentTerm, peer.address.String(), peer.id, nextIndex, int(matchIndex))
					} else {
						nextIndex := max(nextIndex-1, 1)
						r.setNextIndex(peer.id, nextIndex)

						r.Logger.Error().Err(fmt.Errorf("Fail to append entries")).Msgf("Me %s / %s with state %s and term %d failed to append entries of %s / %s because it rejected it, nextIndex: %d", myAddress, myId, state.String(), currentTerm, peer.address.String(), peer.id, nextIndex)
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
			select {
			case <-r.quitCtx.Done():
				r.switchState(Down, true, r.getCurrentTerm())
				return nil, fmt.Errorf("ServerShuttingDown")

			// answer back to the client
			case response := <-responseChan:
				return response.Data, response.Error
			}
		} else {
			leader := r.getLeader().id
			if leader == "" {
				return nil, fmt.Errorf("NoLeader")
			}
			peer := r.getPeerClient(leader)
			if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) {
				if !r.healthyPeer(peer) {
					return nil, fmt.Errorf("NoLeader")
				}

				response, err := peer.rclient.ForwardCommandToLeader(
					context.Background(),
					&raftypb.ForwardCommandToLeaderRequest{
						Command: command,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to forward command to leader %s / %s", peer.address.String(), peer.id)
					return nil, err
				}
				if response.Error == "" {
					err = nil
				} else {
					err = fmt.Errorf("%s", response.Error)
				}
				return response.Data, err
			}
		}
	}
	return nil, fmt.Errorf("CommandNotFound")
}
