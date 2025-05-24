package rafty

// State represent the current status of the raft server.
// The state can only be Leader, Candidate, Follower, ReadOnly, Down
type State uint32

const (
	// Down state is a node that has been unreachable for a long period of time
	Down State = iota

	// ReadOnly state is a node that does not pariticipate into the voting campain
	// It's a passive node that issue no requests on his own but simply respond from the leader
	// This node can never become a follower
	ReadOnly

	// Follower state is a node that participate into the voting campain
	// It's a passive node that issue no requests on his own but simply respond from the leader
	// This node can become a Precandidate if all requirements are available
	Follower

	// Candidate state is a node that participate into the voting campain.
	// It can become a Leader
	Candidate

	// Leader state is a node that was previously a Candidate
	// It received the majority of the votes including itself and get elected as the Leader.
	// It will then handle all client requests
	// Writes requests can only be done on the leader
	Leader
)

// upOrDown is only a helper for the State
// The state can only be Leader, Candidate, Follower, ReadOnly, Down
type upOrDown uint32

const (
	// stepDown state is a helper message for going down
	stepDown upOrDown = iota

	// stepUp state is a helper message for going up
	stepUp
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
		return "readOnly"
	}
	return "down"
}

// String return a human readable state of the raft server for upOrDown
func (s upOrDown) String() string {
	if s == stepUp {
		return "up"
	}
	return "down"
}
