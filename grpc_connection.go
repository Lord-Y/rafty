package rafty

import (
	"sync"
	"sync/atomic"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connectionManager is used to manage all grpc connections to rafty peers
// and to provide clients to the rafty rpc methods
// It is used to ensure that we have only one connection per peer
// and to handle the lifecycle of these connections.
// It is also used to handle the leadership transfer process
// when the current leader is stepping down for maintenance or other reasons.
type connectionManager struct {
	// mu is used to ensure lock concurrency
	mu sync.Mutex

	// connections hold gprc server connection for all clients
	connections map[string]*grpc.ClientConn

	// clients hold gprc rafty client for all clients
	clients map[string]raftypb.RaftyClient

	// Logger expose zerolog so it can be override
	logger *zerolog.Logger

	// id of the current raft server
	id string

	// address is the current address of the raft server
	address string

	// leadershipTransferInProgress is set to true we the actual leader is stepping down for maintenance for example. It's used with TimeoutNow rpc request
	leadershipTransferInProgress *atomic.Bool
}

// getClient return rafty connection client
func (r *connectionManager) getClient(address, id string) raftypb.RaftyClient {
	r.mu.Lock()
	defer r.mu.Unlock()

	if client, ok := r.clients[address]; ok {
		return client
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// in our case for now, err is always nil so no need to check it
	conn, _ := grpc.NewClient(
		address,
		opts...,
	)

	if r.connections == nil {
		if !r.leadershipTransferInProgress.Load() {
			return nil
		}
		r.connections = make(map[string]*grpc.ClientConn)
		r.clients = make(map[string]raftypb.RaftyClient)
	}

	r.connections[address] = conn
	r.clients[address] = raftypb.NewRaftyClient(conn)
	return r.clients[address]
}

// disconnectAllPeers permits to disconnect to all grpc servers
// from which this client is connected to
func (r *connectionManager) disconnectAllPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for address, connection := range r.connections {
		_ = connection.Close()
		delete(r.connections, address)
		delete(r.clients, address)
	}
	r.connections = nil
	r.clients = nil
}
