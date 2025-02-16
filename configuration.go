package rafty

import (
	"net"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
)

type peer struct {
	// Address is the address of a peer node, must be just the ip or ip:port
	Address string `json:"address"`

	// ID of the current peer
	ID string `json:"id"`

	// readOnlyNode allow to statuate if this peer is a read only node
	// This kind of node won't participate into any election campain
	ReadOnlyNode bool `json:"readOnlyNode"`

	// address is the address of a peer node with explicit host and port
	address net.TCPAddr

	// client hold gprc server connection
	client *grpc.ClientConn

	// rclient hold gprc rafty client
	rclient raftypb.RaftyClient
}

// configuration hold configuration related to current server
type configuration struct {
	// ServerMembers hold all current members of the cluster
	ServerMembers []peer

	// newMembers hold all new members of the cluster
	newMembers []peer

	// preCandidatePeers hold the list of the peers
	// that will be use to elect a new leader
	// if no leader has been detected
	preCandidatePeers []peer
}
