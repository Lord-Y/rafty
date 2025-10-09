package rafty

import (
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

const (
	// replicationRetryTimeout is the maximum amount of time to wait for a replication
	// to succeed before timing out
	replicationRetryTimeout = 50 * time.Millisecond

	// replicationMaxRetry is the maximum number of retry to perform before stop retrying
	replicationMaxRetry uint64 = 3

	// maxRound is used during membership request. When reached, it means that the new member
	// is to slow and must retry membership process in order before being promoted
	maxRound uint64 = 10
)

// followerReplication holds all requirements that allow the leader to replicate
// its logs to the current follower
type followerReplication struct {
	// peer holds peer informations
	Peer

	// rafty holds rafty config
	rafty *Rafty

	// newEntry is used by the leader to trigger log replication or hearbeat
	newEntryChan chan bool

	// notifyLeader is used once nextIndex/matchIndex has updated
	notifyLeader chan commitChanConfig

	// replicationStopped is used by the leader
	// to stop ongoing append entries replication
	// or because the leader is stepping down as follower
	// or the leader is shutting down
	replicationStopped atomic.Bool

	// nextIndex is the next log entry to send to that follower
	// initialized to leader last log index + 1
	nextIndex atomic.Uint64

	// matchIndex is the index of the highest log entry
	// known to be replicated on server
	// initialized to 0, increases monotically
	matchIndex atomic.Uint64

	// failures is a counter of RPC call failed
	// It will be used to apply backoff
	failures atomic.Uint64

	// catchup tell us if the follower is currently catching up entries
	catchup atomic.Bool

	// lastContactDate is the last date we heard the follower
	lastContactDate atomic.Value

	// sendSnapshotInProgress tell us if we are currently sending a snapshot to the follower
	sendSnapshotInProgress atomic.Bool
}

// onAppendEntriesRequest holds all requirements that allow the leader
// to replicate entries
type onAppendEntriesRequest struct {
	// totalLogs is the total logs the leader currently have
	totalLogs uint64

	// term is the current term of the leader
	term uint64

	// hearbeat stand here if the leader have to send hearbeat append entries
	heartbeat bool

	// commitIndex is the leader commit index
	commitIndex uint64

	// prevLogIndex is the leader commit index
	prevLogIndex uint64

	// prevLogTerm is the leader commit index
	prevLogTerm uint64

	// entries are logs to use when
	// followerReplication.catchup is set to true
	entries []*raftypb.LogEntry

	// catchup tell us if the follower is currently catching up entries
	catchup bool

	// rpcTimeout is the timeout to use when sending append entries
	rpcTimeout time.Duration

	// membershipChangeInProgress is set to true when membership change is ongoing
	membershipChangeInProgress *atomic.Bool

	// membershipChangeID is the id of the member to take action on
	membershipChangeID string
}
