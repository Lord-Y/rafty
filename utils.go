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
	// 		Str("newState", state.String()).
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
		case ReadOnly:
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
		if !member.ReadOnlyNode && !member.WaitToBePromoted && !member.Decommissioning {
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
			response, _ := client.GetLeader(ctx, &raftypb.GetLeaderRequest{PeerID: r.id, PeerAddress: r.Address.String()})
			if response != nil && response.LeaderAddress != "" && response.LeaderID != "" {
				r.setLeader(leaderMap{
					address: response.LeaderAddress,
					id:      response.LeaderID,
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
