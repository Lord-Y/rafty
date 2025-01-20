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

			go r.reconnect(conn, peerIndex, address)
			if err != nil {
				r.Logger.Err(err).Msgf("Fail to connect to peer %s", address)
				r.mu.Lock()
				if r.clusterSizeCounter > 0 && !r.minimumClusterSizeReach.Load() {
					r.clusterSizeCounter--
				}
				r.mu.Unlock()
				return
			}
			r.mu.Lock()
			if r.clusterSizeCounter+1 < r.MinimumClusterSize && !r.minimumClusterSizeReach.Load() {
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
}

func (r *Rafty) reconnect(conn *grpc.ClientConn, peerIndex int, address string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	ready := false
	myAddress, myId := r.getMyAddress()
	for r.getState() != Down {
		<-ticker.C
		if conn.GetState() != connectivity.Ready {
			ready = false
			conn.Connect()
		}
		time.Sleep(500 * time.Millisecond)
		if conn.GetState() == connectivity.Ready && !ready {
			r.mu.Lock()
			if r.Peers[peerIndex].client == nil || r.Peers[peerIndex].rclient == nil {
				r.Peers[peerIndex].client = conn
				r.Peers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
			}
			r.mu.Unlock()
			ready = true
			r.Logger.Trace().Msgf("Me %s / %s with state %s is reconnected to peer %s ", myAddress, myId, r.getState().String(), address)
		}
	}
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
