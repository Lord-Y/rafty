package rafty

import (
	"context"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

// connectToPeer permits to connect to the specified peer
func (r *Rafty) connectToPeer(address string) {
	peerIndex := r.getPeerSliceIndex(address)
	if peerIndex == -1 {
		return
	}
	r.mu.Lock()
	peer := r.Peers[peerIndex]
	r.mu.Unlock()
	if peer.client == nil {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		//nolint staticcheck
		conn, err := grpc.DialContext(
			ctx,
			address,
			opts...,
		)

		if err != nil {
			r.Logger.Err(err).Msgf("Fail to connect to peer %s", address)
			r.mu.Lock()
			if r.clusterSizeCounter > 0 {
				r.clusterSizeCounter--
			}
			r.mu.Unlock()
			return
		}
		r.mu.Lock()
		if r.clusterSizeCounter+1 < r.MinimumClusterSize {
			r.clusterSizeCounter++
		}
		r.Peers[peerIndex].client = conn
		r.Peers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
		r.mu.Unlock()

		if r.Peers[peerIndex].id == "" {
			r.Logger.Trace().Msgf("Me %s / %s contact peer %s to fetch its id", r.Address.String(), r.ID, r.Peers[peerIndex].address.String())
			ctx := context.Background()
			response, err := r.Peers[peerIndex].rclient.AskNodeID(ctx, &raftypb.AskNodeIDRequest{
				Id:      r.ID,
				Address: r.Address.String(),
			},
				grpc.WaitForReady(true),
				grpc.UseCompressor(gzip.Name),
			)
			if err != nil {
				r.Logger.Error().Err(err).Msgf("Fail to fetch peer %s id", r.Peers[peerIndex].address.String())
				return
			}

			r.mu.Lock()
			r.Peers[peerIndex].id = response.GetPeerID()
			r.mu.Unlock()
		}
		return
	}
}

// healthyPeer permits to check if peer is health to make rpc calls
func (r *Rafty) healthyPeer(peer Peer) bool {
	if peer.client != nil && slices.Contains([]connectivity.State{connectivity.Ready, connectivity.Idle}, peer.client.GetState()) {
		healthClient := healthgrpc.NewHealthClient(peer.client)
		healthCheckRequest := &healthgrpc.HealthCheckRequest{}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()
		response, err := healthClient.Check(ctx, healthCheckRequest)
		if err != nil {
			r.Logger.Error().Err(err).Msgf("Peer %s / %s is unhealthy", peer.address.String(), peer.id)
			return false
		}

		if response.Status != healthgrpc.HealthCheckResponse_SERVING {
			r.Logger.Error().Err(err).Msgf("Peer %s / %s cannot receive rpc calls", peer.address.String(), peer.id)
			return false
		}
		return true
	}
	return false
}

// disconnectToPeers permits to disconnect to all grpc servers
// from which this client is connected to
func (r *Rafty) disconnectToPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.Peers {
		if peer.client != nil {
			// we won't check errors on the targetted server it maybe already down
			_ = peer.client.Close()
			peer.client = nil
		}
	}
}
