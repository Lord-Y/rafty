package rafty

import (
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

// connectToPeer permits to connect to the specified peer
func (r *Rafty) connectToPeer(address string) {
	if r.getState() != Down {
		peerIndex := r.getPeerSliceIndex(address)
		if peerIndex == -1 {
			return
		}
		r.mu.Lock()
		peer := r.configuration.ServerMembers[peerIndex]
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

			go r.reconnect(conn, peerIndex, address)
			if err != nil {
				if r.getState() != Down {
					r.Logger.Error().Err(err).
						Str("address", r.Address.String()).
						Str("id", r.id).
						Str("state", r.getState().String()).
						Str("peerAddress", peer.address.String()).
						Str("peerId", peer.ID).
						Msgf("Fail to connect to peer")
				}
				return
			}
			r.mu.Lock()
			r.configuration.ServerMembers[peerIndex].client = conn
			r.configuration.ServerMembers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
			peer = r.configuration.ServerMembers[peerIndex]
			r.mu.Unlock()

			readOnlyNode := false
			if peer.ID == "" {
				r.Logger.Trace().
					Str("address", r.Address.String()).
					Str("id", r.id).
					Str("state", r.getState().String()).
					Str("peerAddress", peer.address.String()).
					Str("peerId", peer.ID).
					Msgf("Trying to fetch peer id")
				ctx := context.Background()
				response, err := peer.rclient.AskNodeID(
					ctx,
					&raftypb.AskNodeIDRequest{
						Id:      r.id,
						Address: r.Address.String(),
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					if r.getState() != Down {
						r.Logger.Error().Err(err).
							Str("address", r.Address.String()).
							Str("id", r.id).
							Str("state", r.getState().String()).
							Str("peerAddress", peer.address.String()).
							Str("peerId", peer.ID).
							Msgf("Fail to fetch peer id")
					}
					return
				}

				r.mu.Lock()
				r.configuration.ServerMembers[peerIndex].ID = response.PeerID
				r.configuration.ServerMembers[peerIndex].ReadOnlyNode = response.ReadOnlyNode
				r.mu.Unlock()
				if response.LeaderAddress != "" && response.LeaderID != "" {
					r.setLeader(leaderMap{address: response.LeaderAddress, id: response.LeaderID})
				}
				readOnlyNode = response.ReadOnlyNode
			}

			if r.clusterSizeCounter.Load()+1 < r.options.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !readOnlyNode {
				r.clusterSizeCounter.Add(1)
			}
			return
		}
	}
}

func (r *Rafty) reconnect(conn *grpc.ClientConn, peerIndex int, address string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	ready := false
	for r.getState() != Down {
		<-ticker.C
		if conn.GetState() != connectivity.Ready {
			ready = false
			conn.Connect()
		}
		time.Sleep(500 * time.Millisecond)
		if conn.GetState() == connectivity.Ready && !ready {
			r.mu.Lock()
			if r.configuration.ServerMembers[peerIndex].client == nil || r.configuration.ServerMembers[peerIndex].rclient == nil {
				r.configuration.ServerMembers[peerIndex].client = conn
				r.configuration.ServerMembers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
			}
			r.mu.Unlock()
			ready = true
			r.Logger.Trace().
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("state", r.getState().String()).
				Str("peerAddress", address).
				Msgf("Reconnected to peer")
		}
	}
}

// disconnectToPeers permits to disconnect to all grpc servers
// from which this client is connected to
func (r *Rafty) disconnectToPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.configuration.ServerMembers {
		if peer.client != nil {
			// we won't check errors on the targetted server it maybe already down
			_ = peer.client.Close()
			peer.client = nil
		}
	}
}
