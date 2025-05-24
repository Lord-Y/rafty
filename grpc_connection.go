package rafty

import (
	"sync"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	conn, err := grpc.NewClient(
		address,
		opts...,
	)

	if err != nil {
		r.logger.Error().Err(err).
			Str("address", r.address).
			Str("id", r.id).
			Str("peerAddress", address).
			Str("peerId", id).
			Msgf("Fail to create new peer client")
		return nil
	}

	// sometimes, during unit testing, the map is nil
	// so let's avoid failure
	if r.connections == nil {
		return nil
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
