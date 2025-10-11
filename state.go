package rafty

// String return a human readable state of the raft server
func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	case Follower:
		return "follower"
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
