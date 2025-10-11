package rafty

import (
	"net"
)

// Peer holds configuration needed by a peer node
type Peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string `json:"address"`

	// ID of the current peer
	ID string `json:"id"`

	// IsVoter statuates if this peer is a voting member node.
	// When set to false, this node won't participate into any election campaign
	IsVoter bool `json:"isVoter"`

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr

	// WaitToBePromoted is a boolean when set to true make sure the node
	// can fully participate in raft operations.
	// It's used when using AddVoter or AddNonVoter
	WaitToBePromoted bool `json:"waitToBePromoted"`

	// Decommissioning is a boolean when set to true will allow devops
	// to put this node on maintenance or to lately send a membership
	// removal command to be safely be removed from the cluster.
	// DON'T confuse it with WaitToBePromoted flag
	Decommissioning bool `json:"decommissioning"`
}

// Configuration holds configuration related to current server
type Configuration struct {
	// ServerMembers holds all current members of the cluster
	ServerMembers []Peer `json:"serverMembers"`
}
