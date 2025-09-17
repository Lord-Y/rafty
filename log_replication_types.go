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

// appendEntriesResponse is used to answer back to the client
// when fetching or appling log entries
type appendEntriesResponse struct {
	Data  []byte
	Error error
}

// triggerAppendEntries will be used by triggerAppendEntriesChan
type triggerAppendEntries struct {
	command      []byte
	responseChan chan appendEntriesResponse
}

// followerReplication hold all requirements that allow the leader to replicate
// its logs to the current follower
type followerReplication struct {
	// peer holds peer informations
	Peer

	// rafty holds rafty config
	rafty *Rafty

	// newEntry is used by the leader every times it received
	// a new log entry
	newEntryChan chan *onAppendEntriesRequest

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

// onAppendEntriesRequest hold all requirements that allow the leader
// to replicate entries
type onAppendEntriesRequest struct {
	// majority represent how many nodes the append entries request
	// has been successful.
	// It will then be used with totalMajority var to determine whether
	// logs must be committed on disk.
	// It only incremented by voters
	majority atomic.Uint64

	// quorum is the minimum number to be reached before commit logs
	// to disk
	quorum uint64

	// totalFollowers hold the total number of nodes for which the leader has sent
	// the append entries request
	totalFollowers uint64

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

	// committed tell us if the log has already been committed on disk
	committed atomic.Bool

	// leaderVolatileStateUpdated tell us if the leader volatile state has already been updated
	leaderVolatileStateUpdated atomic.Bool

	// replyToClientChan is a boolean that allow the leader
	// to reply the client by using replyClientChan var
	replyToClient bool

	// replyClientChan is used by replyToClientChan
	replyToClientChan chan appendEntriesResponse

	// replyToForwardedCommand is a boolean that allow the leader
	// to reply the client by using replyToForwardedCommandChan var
	replyToForwardedCommand bool

	// replyToForwardedCommandChan is used by replyToForwardedCommand
	replyToForwardedCommandChan chan<- RPCResponse

	// uuid is used only for debugging.
	// It helps to differenciate append entries requests
	uuid string

	// entries are logs to use when
	// followerReplication.catchup is set to true
	entries []*raftypb.LogEntry

	// catchup tell us if the follower is currently catching up entries.
	// It will also be used when a new leader is promoted with term > 1
	catchup bool

	// rpcTimeout is the timeout to use when sending append entries
	rpcTimeout time.Duration

	// membershipChangeInProgress is set to true when membership change is ongoing
	membershipChangeInProgress *atomic.Bool

	// membershipChangeCommitted is set to true when log config has been replicated
	// to the majority of servers
	membershipChangeCommitted atomic.Bool

	// membershipChangeID is the id of the member to take action on.
	// When filled membershipChangeCommitted can be updated
	membershipChangeID string
}
