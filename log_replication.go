package rafty

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/Lord-Y/rafty/grpcrequests"
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
func (r *Rafty) appendEntries(clientChan chan appendEntriesResponse, replyToClient, replyToClientGRPC bool) {
	currentTerm := r.getCurrentTerm()
	commitIndex := r.getCommitIndex()
	myAddress, myId := r.getMyAddress()
	state := r.getState()
	r.mu.Lock()
	peers := r.Peers
	totalPeers := len(peers)
	totalLogs := len(r.log) - 1
	r.mu.Unlock()
	var majority atomic.Uint64
	totalMajority := (totalPeers / 2) + 1

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
						majority.Add(1)
						if totalLogs > 0 {
							r.setNextAndMatchIndex(peer.id, max(prevLogIndex+uint64(totalEntries)+1, 1), nextIndex-1)
						} else {
							r.setNextAndMatchIndex(peer.id, 1, 0)
						}

						if int(majority.Load()) >= totalMajority {
							if replyToClient {
								clientChan <- appendEntriesResponse{}
							}
							if replyToClientGRPC {
								r.rpcForwardCommandToLeaderRequestChanWritter <- &grpcrequests.ForwardCommandToLeaderResponse{}
							}
							r.Logger.Debug().Msgf("Me %s / %s with state %s and term %d successfully append entries to the majority of servers %d >= %d", myAddress, myId, state.String(), currentTerm, int(majority.Load()), totalMajority)
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
			case <-r.quit:
				r.switchState(Down, false, r.getCurrentTerm())
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
				r.Logger.Info().Msgf("peer.rclient.ForwardCommandToLeader %s", string(command))
				response, err := peer.rclient.ForwardCommandToLeader(
					context.Background(),
					&grpcrequests.ForwardCommandToLeaderRequest{
						Command: command,
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to get forward command to leader leader %s / %s", peer.address.String(), peer.id)
					return nil, err
				}
				if response.Error == "" {
					err = nil
				} else {
					err = fmt.Errorf("%s", response.Error)
				}
				return response.Data, err
			}
			r.Logger.Info().Msgf("peer leader not healthy")
		}
	}
	return nil, fmt.Errorf("CommandNotFound")
}
