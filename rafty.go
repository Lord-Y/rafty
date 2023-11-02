package rafty

import "net"

// State represent the current status of the raft server.
// The state can only be Leader, Candidate, Follower, ReadOnly, Down
type State uint16

const (
	// Down state is a node that has been unreachable for a long period of time
	Down State = iota

	// ReadOnly state is a node that does not pariticipate into the voting campain
	// It's a passive node that issue not requests on his own be simply respond answer from the leader
	// This node can never become a follower
	ReadOnly

	// Follower state is a node that participate into the voting campain
	// It's a passive node that issue not requests on his own be simply respond answer from the leader
	// This node can become a candidate if all requirements are available
	Follower

	// Candidate state is a node is a node that participate into the voting campain.
	// It that can become a Leader
	Candidate

	// Candidate state is a node that was previously a Candidate
	// It received the majority of the votes including itself and get elected as the Leader.
	// It will then handle all client requests
	// Writes requests can only be done on the leader
	Leader
)

// String return a human readable state of the raft server
func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
	case ReadOnly:
		return "readonly"
	}
	return "down"
}

type Status struct {
	// ID of the current raft server
	ID string

	// Addr is the current address of the raft server
	Addr net.Addr

	// State of the current raft server
	// Can only be Leader, Candidate, Follower, ReadOnly, Down
	State

	// CurrentTerm is latest term seen during the voting campain
	CurrentTerm uint64

	// CurrentCommitIndex is the index of the highest log entry know to be commited
	CurrentCommitIndex uint64

	// LastApplied is the index of the highest log entry applied to the current raft server
	LastApplied uint64
}
