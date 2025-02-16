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
				r.Logger.Err(err).Msgf("Fail to connect to peer %s", address)
				return
			}
			r.mu.Lock()
			r.configuration.ServerMembers[peerIndex].client = conn
			r.configuration.ServerMembers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
			r.mu.Unlock()

			readOnlyNode := false
			if r.configuration.ServerMembers[peerIndex].ID == "" {
				r.Logger.Trace().Msgf("Me %s / %s contact peer %s to fetch its id", r.Address.String(), r.ID, r.configuration.ServerMembers[peerIndex].address.String())
				ctx := context.Background()
				response, err := r.configuration.ServerMembers[peerIndex].rclient.AskNodeID(
					ctx,
					&raftypb.AskNodeIDRequest{
						Id:      r.ID,
						Address: r.Address.String(),
					},
					grpc.WaitForReady(true),
					grpc.UseCompressor(gzip.Name),
				)
				if err != nil {
					r.Logger.Error().Err(err).Msgf("Fail to fetch peer %s id", r.configuration.ServerMembers[peerIndex].address.String())
					return
				}

				r.mu.Lock()
				r.configuration.ServerMembers[peerIndex].ID = response.GetPeerID()
				r.configuration.ServerMembers[peerIndex].ReadOnlyNode = response.GetReadOnlyNode()
				r.mu.Unlock()
				if response.GetLeaderID() != "" && response.GetLeaderAddress() != "" {
					r.saveLeaderInformations(leaderMap{id: response.GetLeaderID(), address: response.GetLeaderAddress()})
				}
				readOnlyNode = response.GetReadOnlyNode()
			}

			if r.clusterSizeCounter+1 < r.MinimumClusterSize && !r.minimumClusterSizeReach.Load() && !readOnlyNode {
				r.clusterSizeCounter++
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
			if r.configuration.ServerMembers[peerIndex].client == nil || r.configuration.ServerMembers[peerIndex].rclient == nil {
				r.configuration.ServerMembers[peerIndex].client = conn
				r.configuration.ServerMembers[peerIndex].rclient = raftypb.NewRaftyClient(conn)
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
	for _, peer := range r.configuration.ServerMembers {
		if peer.client != nil {
			// we won't check errors on the targetted server it maybe already down
			_ = peer.client.Close()
			peer.client = nil
		}
	}
}
