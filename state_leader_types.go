package rafty

import (
	"sync"
	"sync/atomic"
	"time"
)

// leader holds all requirements by a node in leader state
type leader struct {
	// rafty holds rafty config
	rafty *Rafty

	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// murw is used to ensure r/w lock concurrency
	murw sync.RWMutex

	// leaseTimer is how long the leader will still be the leader.
	// If the quorum of voters is unreachable, the it will step down as follower
	leaseTimer *time.Ticker

	// leaseDuration is used to set leaseTimer ticker
	leaseDuration time.Duration

	// followerReplication holds all requirements that will
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

	// commitChan is the leader commit chan used by followers
	// to indicate that their nextIndex/matchIndex has been updated
	commitChan chan commitChanConfig

	// commitIndexWatcher is the map built by replicateLog
	// with the index of the that need to be replicated and
	// its related informations
	commitIndexWatcher map[uint64]*indexWatcher

	// readIndexWatchers is the map built by replicateLog
	// with the uuid of the that need for linearizable read calls
	readIndexWatchers map[string]*readIndexWatcher
}

// replicateLogConfig is a struct holding all requirements
// to replicate logs
type replicateLogConfig struct {
	// heartbeat is a boolean indicating to it's use to keep leadership
	heartbeat bool

	// logType represent the kind of the log
	logType LogKind

	// command is the data to be replicated if not nil
	command []byte

	// source indicated if it's a submitCommand or a forwarded one
	source string

	// clientChan is a boolean that allow the leader
	// to reply the client by using clientChan var
	client bool

	// clientChan is used by client var
	clientChan chan<- RPCResponse

	// membershipChange holds requirements related to membership changes
	membershipChange struct {
		// action is the membership change action to perform
		action MembershipChange

		// member is only used during membership change
		member Peer
	}
}

// indexWatcher is a struct holding informations about
// the entry index we need to watch for
type indexWatcher struct {
	// majority represent how many nodes the append entries request
	// has been successful.
	// It will then be used with totalFollowers var to determine whether
	// logs must be committed on disk.
	majority atomic.Uint64

	// quorum is the minimum number to be reached before commit logs
	// to disk
	quorum uint64

	// totalFollowers holds the total number of nodes for which the leader has sent
	// the append entries request
	totalFollowers uint64

	// committed tell if the log entry has been committed
	// as the quorum as been reach.
	// When set to true, it's ready to be removed from commitIndexWatcher
	committed atomic.Bool

	// term is the term of the entry
	term uint64

	// LogKind represent the kind of the log
	logType LogKind

	// uuid is used only for debugging.
	// It helps to differenciate append entries requests
	uuid string

	// logs are entries that need to be replicated and applied to the fsm
	logs []*LogEntry

	// membershipChange holds requirements related to membership changes
	membershipChange struct {
		// action is the membership change action to perform
		action MembershipChange

		// member is only used during membership change
		member Peer
	}

	// source indicated if it's a submitCommand or a forwarded one
	source string

	// clientChan is a boolean that allow the leader
	// to reply the client by using clientChan var
	client bool

	// clientChan is used by client var
	clientChan chan<- RPCResponse
}

// commitChanConfig holds requirements to use
// once follower updated its nextIndex / matchIndex.
// We use this informations in order to avoid looping
// over follower list and find requirements
type commitChanConfig struct {
	// id is the follower id
	id string

	// matchIndex is the index of the highest log entry known to be replicated on server initialized to 0, increases monotically
	matchIndex uint64

	// readIndex is used to indicate if we need to perform a linearizable
	// read. It's a special heartbeat
	readIndex bool

	// uuid helps to differenciate append entries requests
	uuid string
}

// readIndexWatcher is a struct holding informations about
// the read index we need to watch for
type readIndexWatcher struct {
	// readIndex is the commit index we perform hearbeat append entries with
	// to verify that we are still the leader and synchronized with followers
	readIndex uint64

	// majority represent how many nodes the heartbeat append entries request
	// has been successful.
	// It will then be used with totalFollowers var to determine whether
	// we can reply client with fsm response.
	majority atomic.Uint64

	// quorum is the minimum number to be reached replying to client
	// to disk
	quorum uint64

	// quorumReached is only a helper to prevent
	// answering the client multiple times
	quorumReached atomic.Bool

	// totalFollowers holds the total number of nodes for which the leader has sent
	// the append entries request
	totalFollowers uint64

	// term is the term of the entry
	term uint64

	// uuid helps to differenciate append entries requests
	uuid string

	// logType represent the kind of the log
	logType LogKind

	// command is the data to be replicated if not nil
	command []byte

	// clientChan is used by client var
	clientChan chan<- RPCResponse
}
