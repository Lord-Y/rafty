package rafty

import (
	"net"
)

type peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string `json:"address"`

	// ID of the current peer
	ID string `json:"id"`

	// readOnlyNode allow to statuate if this peer is a read only node
	// This kind of node won't participate into any election campaign
	ReadOnlyNode bool `json:"readOnlyNode"`

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

// configuration hold configuration related to current server
type configuration struct {
	// ServerMembers hold all current members of the cluster
	ServerMembers []peer
}
