package rafty

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// getState permits to retrieve node state
// The purpose of this helper is to make sure
// we don't have any race conditions when
// multiple goroutines are used by following
// https://go.dev/ref/mem method
// espacially https://pkg.go.dev/sync/atomic package
func (r *Rafty) getState() State {
	addr := (*uint32)(&r.State)
	return State(atomic.LoadUint32(addr))
}

// getVotedFor permits to retrieve node votedFor and term value
func (r *Rafty) getVotedFor() (votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	votedFor, term = r.votedFor, r.votedForTerm.Load()
	return
}

// setVotedFor permits to set node votedFor and term value
func (r *Rafty) setVotedFor(votedFor string, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = votedFor
	r.votedForTerm.Store(term)
}

// getLeader permits to retrieve current leader informations
func (r *Rafty) getLeader() leaderMap {
	xx, ok := r.leader.Load("leader")
	if ok {
		return xx.(leaderMap)
	}
	return leaderMap{}
}

// setLeader permits to set current leader informations
func (r *Rafty) setLeader(newLeader leaderMap) {
	var currentLeader leaderMap
	xx, ok := r.leader.Load("leader")
	if ok {
		currentLeader = xx.(leaderMap)
		r.leader.Store("oldLeader", currentLeader)
	}
	if newLeader == (leaderMap{}) {
		r.leader.Delete("leader")
		return
	}
	if currentLeader != newLeader {
		r.leader.Store("leader", newLeader)
	}
}

// getPeers permits to retrieve all peers from the cluster
// except current node
func (r *Rafty) getPeers() (peers []peer, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	peers = slices.Clone(r.configuration.ServerMembers)
	return peers, len(peers)
}

// getAllPeers permits to retrieve all members of the cluster
// including current node
func (r *Rafty) getAllPeers() (peers []peer, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	peers = append(peers, peer{
		ID:               r.id,
		Address:          r.Address.String(),
		address:          getNetAddress(r.Address.String()),
		WaitToBePromoted: r.waitToBePromoted.Load(),
		Decommissioning:  r.decommissioning.Load(),
	})
	// we need to have the current node first
	peers = slices.Concat(peers, slices.Clone(r.configuration.ServerMembers))
	return peers, len(peers)
}

// IsRunning return a boolean tell if the node is running
func (r *Rafty) IsRunning() bool {
	return r.isRunning.Load()
}

// parsePeers will parse all peers to validate their addresses.
// When invalid, an error will be returned
func (r *Rafty) parsePeers() error {
	var uniqPeers []peer
	for _, server := range r.configuration.ServerMembers {
		var addr net.TCPAddr
		host, port, err := net.SplitHostPort(server.Address)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				return err
			}
			if port == "" {
				addr = net.TCPAddr{
					IP:   net.ParseIP(server.Address),
					Port: int(GRPCPort),
				}
				if r.Address.String() != addr.String() {
					uniqPeers = append(uniqPeers, peer{
						Address: addr.String(),
						address: addr,
						ID:      server.ID,
					})
				}
			}
		} else {
			p, err := strconv.Atoi(port)
			if err != nil {
				return err
			}
			addr = net.TCPAddr{
				IP:   net.ParseIP(host),
				Port: p,
			}
			if r.Address.String() != addr.String() {
				uniqPeers = append(uniqPeers, peer{
					Address: addr.String(),
					address: addr,
					ID:      server.ID,
				})
			}
		}
	}
	r.mu.Lock()
	r.configuration.ServerMembers = uniqPeers
	r.mu.Unlock()
	return nil
}

// switchState permits to switch to the mentionned state
// and print a nice message if needed
func (r *Rafty) switchState(newState State, upOrDown upOrDown, niceMessage bool, currentTerm uint64) {
	// _, file, no, ok := runtime.Caller(1)
	// if ok {
	// 	r.Logger.Info().
	// 		Str("address", r.Address.String()).
	// 		Str("newState", newState.String()).
	// 		Msgf("Called from %s#%d", file, no)
	// }
	addr := (*uint32)(&r.State)
	currentState := State(atomic.LoadUint32(addr))

	if currentState == Down && !r.isRunning.Load() && newState != Down {
		return
	}

	if currentState == newState {
		return
	}
	atomic.StoreUint32(addr, uint32(newState))

	if newState == Down {
		r.isRunning.Store(false)
		r.setLeader(leaderMap{})
	}

	if niceMessage {
		switch newState {
		case ReadReplica:
		case Down:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", newState.String()).
				Msgf("Shutting down with term %d", currentTerm)
		case Candidate:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", newState.String()).
				Msgf("Stepping %s as %s with term %d", upOrDown.String(), newState.String(), currentTerm+1)
		default:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Stepping %s as %s with term %d", upOrDown.String(), newState.String(), currentTerm)
		}
	}
}

// min return the minimum value based on provided values
func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// max return the maximum value based on provided values
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// backoff is used to calculate exponential backoff
// time to wait before processing again
func backoff(wait time.Duration, failures, maxFailures uint64) time.Duration {
	power := min(failures, maxFailures)
	for power > 2 {
		wait *= 2
		power--
	}
	return wait
}

// quorum calculate the number to be reach to start election
func (r *Rafty) quorum() int {
	r.murw.RLock()
	defer r.murw.RUnlock()
	voter := 0
	for _, member := range r.configuration.ServerMembers {
		if !member.ReadReplica && !member.WaitToBePromoted && !member.Decommissioning {
			voter++
		}
	}
	return voter/2 + 1
}

// calculateMaxRangeLogIndex return the max log index limit
// needed when leader need to send catchup entries to followers
func calculateMaxRangeLogIndex(totalLogs, maxAppendEntries, initialIndex uint) (limit uint, snapshot bool) {
	if totalLogs > maxAppendEntries {
		limit = maxAppendEntries
		snapshot = true
	} else {
		limit = totalLogs
	}

	if initialIndex > limit {
		limit += initialIndex
		snapshot = false
		return
	}
	return
}

// getNetAddress returns a net.TCPAddr from a string address
func getNetAddress(address string) net.TCPAddr {
	host, port, _ := net.SplitHostPort(address)
	p, _ := strconv.Atoi(port)
	return net.TCPAddr{
		IP:   net.ParseIP(host),
		Port: p,
	}
}

// isPartOfTheCluster will check if provided peer is already in configuration
// and so be part of the cluster
func (r *Rafty) isPartOfTheCluster(member peer) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index := slices.IndexFunc(r.configuration.ServerMembers, func(p peer) bool {
		return p.Address == member.Address || p.Address == member.Address && p.ID == member.ID
	}); index == -1 {
		return false
	}
	return true
}

// isPartOfTheCluster will check if provided peer is already in configuration
// and so be part of the cluster
func isPartOfTheCluster(list []peer, member peer) bool {
	if index := slices.IndexFunc(list, func(p peer) bool {
		return p.Address == member.Address && p.ID == member.ID
	}); index == -1 {
		return false
	}
	return true
}

// waitForLeader is an helper that allow us to wait and find
// the leader before. It's actually used when we need to send LeaveOnTerminate
// to fully remove itself from the cluster.
// It can also be used for other purposes
func (r *Rafty) waitForLeader() bool {
	peers, _ := r.getPeers()
	for _, peer := range peers {
		client := r.connectionManager.getClient(peer.address.String(), peer.ID)
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			response, _ := client.GetLeader(ctx, &raftypb.GetLeaderRequest{PeerId: r.id, PeerAddress: r.Address.String()})
			if response != nil && response.LeaderAddress != "" && response.LeaderId != "" {
				r.setLeader(leaderMap{
					address: response.LeaderAddress,
					id:      response.LeaderId,
				})
				return true
			}
		}
	}
	return false
}

// updateServerMembers will update configuration server members
// list
func (r *Rafty) updateServerMembers(peers []peer) {
	r.wg.Add(1)
	defer r.wg.Done()
	r.mu.Lock()
	defer r.mu.Unlock()

	// find and update myself
	if index := slices.IndexFunc(peers, func(p peer) bool {
		return p.Address == r.Address.String() && p.ID == r.id
	}); index != -1 {
		r.waitToBePromoted.Store(peers[index].WaitToBePromoted)
		r.decommissioning.Store(peers[index].Decommissioning)
	}

	// remove myself from the list and update members
	// shallow clone is necessary in order to avoid bad suprises during unit testing
	r.configuration.ServerMembers = slices.DeleteFunc(slices.Clone(peers), func(p peer) bool {
		return p.Address == r.Address.String()
	})
	// temporary update parsed address. We will need to remove this field later
	for i := range r.configuration.ServerMembers {
		r.configuration.ServerMembers[i].address = getNetAddress(r.configuration.ServerMembers[i].Address)
	}
}
