package rafty

import (
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
func (r *Rafty) switchState(state State, upOrDown upOrDown, niceMessage bool, currentTerm uint64) {
	// _, file, no, ok := runtime.Caller(1)
	// if ok {
	// 	r.Logger.Info().
	// 		Str("address", r.Address.String()).
	// 		Str("newState", state.String()).
	// 		Msgf("Called from %s#%d", file, no)
	// }
	if r.getState() == state || (r.isRunning.Load() && r.getState() == Down) {
		return
	}
	addr := (*uint32)(&r.State)
	atomic.StoreUint32(addr, uint32(state))

	if state == Down {
		r.setLeader(leaderMap{})
	}

	if niceMessage {
		switch state {
		case ReadOnly:
		case Down:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Shutting down with term %d", currentTerm)
		case Candidate:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Stepping %s as %s with term %d", upOrDown.String(), state.String(), currentTerm+1)
		default:
			r.Logger.Info().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Msgf("Stepping %s as %s with term %d", upOrDown.String(), state.String(), currentTerm)
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
		if !member.ReadOnlyNode {
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
