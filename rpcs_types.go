package rafty

import "time"

// RPCType is used to build rpc requests
type RPCType uint8

const (
	// AskNodeID will be used to ask node id
	AskNodeID RPCType = iota

	// GetLeader will be used to ask who is the leader
	GetLeader

	// PreVoteRequest is used during pre vote request
	PreVoteRequest

	// VoteRequest is used during Vote request
	VoteRequest

	// AppendEntriesRequest is used by the leader to replicate logs to followers
	AppendEntriesRequest

	// AppendEntriesReplicationRequest is used during append entries request on follower side
	AppendEntriesReplicationRequest

	// ForwardCommandToLeader is used to forward command to leader
	ForwardCommandToLeader

	// TimeoutNow is used during leadership transfer
	TimeoutNowRequest

	// BootstrapClusterRequest is used to bootstrap the cluster
	BootstrapClusterRequest

	// InstallSnapshotRequest is used to install snapshot in the cluster
	InstallSnapshotRequest
)

// RPCRequest is used by chans in order to manage rpc requests
type RPCRequest struct {
	RPCType      RPCType
	Request      any
	Timeout      time.Duration
	ResponseChan chan<- RPCResponse
}

// RPCResponse  is used by RPCRequest in order to reply to rpc requests
type RPCResponse struct {
	TargetPeer Peer
	Response   any
	Error      error
}

// RPCAskNodeIDRequest holds the requirements to ask node id
type RPCAskNodeIDRequest struct {
	Id, Address string
}

// RPCAskNodeIDResponse holds the response from RPCAskNodeIDRequest
type RPCAskNodeIDResponse struct {
	LeaderID, LeaderAddress, PeerID string
	ReadReplica, AskForMembership   bool
}

// RPCGetLeaderRequest holds the requirements to get the leader
type RPCGetLeaderRequest struct {
	PeerID, PeerAddress string
	TotalPeers          int
}

// RPCGetLeaderResponse holds the response from RPCGetLeaderRequest
type RPCGetLeaderResponse struct {
	LeaderID, LeaderAddress, PeerID string
	TotalPeers                      int
	AskForMembership                bool
}

// RPCPreVoteRequest holds the requirements to send pre vote requests
type RPCPreVoteRequest struct {
	Id          string
	CurrentTerm uint64
}

// RPCPreVoteResponse holds the response from RPCPreVoteRequest
type RPCPreVoteResponse struct {
	PeerID                     string
	RequesterTerm, CurrentTerm uint64
	Granted                    bool
}

// RPCVoteRequest holds the requirements to send vote requests
type RPCVoteRequest struct {
	CandidateId, CandidateAddress          string
	CurrentTerm, LastLogIndex, LastLogTerm uint64
	CandidateForLeadershipTransfer         bool
}

// RPCVoteResponse holds the response from RPCVoteRequest
type RPCVoteResponse struct {
	PeerID                     string
	RequesterTerm, CurrentTerm uint64
	Granted                    bool
}

// RPCTimeoutNowRequest holds the requirements to send timeout now requests
// for leadership transfer
type RPCTimeoutNowRequest struct{}

// RPCTimeoutNowResponse holds the response from RPCTimeoutNowRequest
type RPCTimeoutNowResponse struct {
	Success bool
}

// RPCMembershipChangeRequest holds the requirements to send membership requests
type RPCMembershipChangeRequest struct {
	Address, Id               string
	ReadReplica               bool
	Action                    uint32
	LastLogIndex, LastLogTerm uint64
}

// RPCMembershipChangeResponse holds the response from RPCMembershipChangeRequest
type RPCMembershipChangeResponse struct {
	ActionPerformed, Response uint32
	LeaderID, LeaderAddress   string
	Peers                     []Peer
	Success                   bool
}
