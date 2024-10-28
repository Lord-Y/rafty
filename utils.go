package rafty

import (
	"context"
	"net"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func (r *Rafty) parsePeers() error {
	var uniqPeers []Peer
	for _, peer := range r.Peers {
		var addr net.TCPAddr
		host, port, err := net.SplitHostPort(peer.Address)
		if err != nil {
			if port == "" {
				addr = net.TCPAddr{
					IP:   net.ParseIP(peer.Address),
					Port: int(GRPCPort),
				}
				if r.Status.Address.String() != addr.String() {
					uniqPeers = append(uniqPeers, Peer{
						Address: addr.String(),
						address: addr,
					})
				}
			} else {
				return err
			}
		} else {
			p, err := strconv.Atoi(port)
			if err != nil {
				return err
			}
			addr = net.TCPAddr{
				IP:   net.ParseIP(host),
				Port: p,
			}
			if r.Status.Address.String() != addr.String() {
				uniqPeers = append(uniqPeers, Peer{
					Address: addr.String(),
					address: addr,
				})
			}
		}
	}
	r.mu.Lock()
	r.Peers = uniqPeers
	r.mu.Unlock()
	return nil
}

// getPeerSliceIndex will be used to retrieve
// the index of the peer by providing its address
func (r *Rafty) getPeerSliceIndex(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.address.String() == addr
	})
	if index != -1 {
		return index
	}
	return 0
}

// checkIfPeerInSliceIndex will be used to check
// if peer is present in the peer list
func (r *Rafty) checkIfPeerInSliceIndex(preVote bool, addr string) bool {
	if preVote {
		index := slices.IndexFunc(r.PreCandidatePeers, func(p Peer) bool {
			return p.address.String() == addr
		})
		return index != -1
	}

	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.address.String() == addr
	})
	return index != -1
}

func (r *Rafty) loopingOverNodeState() {
	for r.getState() != Down {
		select {
		// stop go routine when os signal is receive or ctrl+c
		case <-r.quit:
			r.switchState(Down, false, r.getCurrentTerm())
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runAsFollower()
		case Candidate:
			r.runAsCandidate()
		case Leader:
			r.runAsLeader()
		}
	}
}

// switchState permits to switch to the mentionned state
// and print a nice message if needed
func (r *Rafty) switchState(state State, niceMessage bool, currentTerm uint64) {
	if r.getState() == state {
		return
	}
	addr := (*uint32)(&r.State)
	atomic.StoreUint32(addr, uint32(state))

	if state == Follower {
		r.volatileStateInitialized.Store(false)
		// r.resetQuorum()
	}

	if niceMessage {
		myAddress, myId := r.getMyAddress()
		switch state {
		case Follower:
			r.Logger.Info().Msgf("Me %s / %s stepping down as %s for term %d", myAddress, myId, state, currentTerm)
		case Candidate:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", myAddress, myId, state, currentTerm)
		case Leader:
			r.stopElectionTimer(true, true)
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", myAddress, myId, state, currentTerm)
		}
	}
}

// logState permits to log mentionned state
// with a nice message if needed
func (r *Rafty) logState(state State, niceMessage bool, currentTerm uint64) {
	if r.getState() == state {
		return
	}

	if niceMessage {
		switch state {
		case Follower:
			r.Logger.Info().Msgf("Me %s / %s stepping down as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		case Candidate:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		case Leader:
			r.Logger.Info().Msgf("Me %s / %s stepping up as %s for term %d", r.Address.String(), r.ID, state, currentTerm)
		}
	}
}

// saveLeaderInformations permits to copy leader informations to oldLeader
// for later use and then set it to nil
func (r *Rafty) saveLeaderInformations(newLeader leaderMap) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.State == Leader {
		if r.oldLeader == nil {
			leader := leaderMap{
				id:      r.ID,
				address: r.Address.String(),
			}
			r.oldLeader = &leader
			return
		}
		r.oldLeader.address = r.Address.String()
		r.oldLeader.id = r.ID
		return
	}
	if r.leader == &newLeader && r.leader != nil {
		return
	}
	r.oldLeader = r.leader
	r.leader = &newLeader
}

// connectToPeer permits to connect to the specified peer
func (r *Rafty) connectToPeer(address string) {
	peerIndex := r.getPeerSliceIndex(address)
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

// min return the minimum value based on provided values
func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// max return the maximum value based on provided values
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// getPeerClient will be used to retrieve
// the index of the peer and return its related struct
func (r *Rafty) getPeerClient(peerId string) Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := slices.IndexFunc(r.Peers, func(p Peer) bool {
		return p.id == peerId
	})
	if index != -1 {
		return r.Peers[index]
	}
	return Peer{}
}
