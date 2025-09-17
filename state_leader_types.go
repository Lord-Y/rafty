package rafty

import (
	"sync"
	"sync/atomic"
	"time"
)

// leader hold all requirements by a node in leader state
type leader struct {
	// rafty holds rafty config
	rafty *Rafty

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// leaseTimer is how long the leader will still be the leader.
	// If the quorum of voters is unreachable, the it will step down as follower
	leaseTimer *time.Ticker

	// leaseDuration is used to set leaseTimer ticker
	leaseDuration time.Duration

	// followerReplication hold all requirements that will
	// be used by the leader to replicate append entries
	followerReplication map[string]*followerReplication

	// totalFollowers is the total number of followers
	// used during the replication. It must be update when adding or removing
	// a follower
	totalFollowers atomic.Uint64

	// disableHeartBeat is set to true temporary while sending new entries
	disableHeartBeat atomic.Bool

	// leadershipTransferTimer is used for leadership transfer
	leadershipTransferTimer *time.Ticker

	// leadershipTransferDuration is a helper used by leadershipTransferTimer
	leadershipTransferDuration time.Duration

	// leadershipTransferChan will receive rpc response
	leadershipTransferChan chan RPCResponse

	// leadershipTransferChanClosed is a helper telling us if leadershipTransferChan is closed
	// to stop leadershipTransferLoop func
	leadershipTransferChanClosed atomic.Bool

	// leadershipTransferInProgress is used to check if a leadership transfer is in progress
	// an is set to true when leadershipTransferTimer is resetted.
	// When leadershipTransferTimer times out, the leadership transfer will be stopped
	leadershipTransferInProgress atomic.Bool

	// membershipChangeInProgress is set to true when membership change is ongoing
	membershipChangeInProgress atomic.Bool

	// singleServerNewEntryChan is used by the leader every times it received
	// a new log entry
	singleServerNewEntryChan chan *onAppendEntriesRequest

	// singleServerReplicationStopped is used by the leader
	// to stop ongoing append entries replication
	singleServerReplicationStopped atomic.Bool
}
