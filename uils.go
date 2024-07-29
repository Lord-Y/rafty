package rafty

import (
	"net"
	"strconv"

	"github.com/Lord-Y/rafty/grpcrequests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (r *Rafty) parsePeers() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var cleanPeers []*Peer // means without duplicate entries
	for _, v := range r.Peers {
		var addr net.TCPAddr
		host, port, err := net.SplitHostPort(v.Address)
		if err != nil {
			if port == "" {
				addr = net.TCPAddr{
					IP:   net.ParseIP(v.Address),
					Port: int(GRPCPort),
				}
				if r.Status.Address.String() != addr.String() {
					cleanPeers = append(cleanPeers, &Peer{
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
				cleanPeers = append(cleanPeers, &Peer{
					Address: addr.String(),
					address: addr,
				})
			}
		}
	}
	r.Peers = cleanPeers
	return nil
}

// getPeerSliceIndex will be used to retrieve
// the index of the peer by providing its address
func (r *Rafty) getPeerSliceIndex(addr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.Peers {
		if r.Peers[k].address.String() == addr {
			return k
		}
	}
	return 0
}

// checkIfPeerInSliceIndex will be used to check
// if peer is present in the peer list
func (r *Rafty) checkIfPeerInSliceIndex(preVote bool, addr string) bool {
	if preVote {
		for k := range r.PreCandidatePeers {
			if r.PreCandidatePeers[k].address.String() == addr {
				return true
			}
		}
		return false
	}

	for k := range r.Peers {
		if r.Peers[k].address.String() == addr {
			return true
		}
	}
	return false
}

// switchPeerToUpOrDown allow the actual node to close connection
// to the peer or set isDown to false to bring it up
func (r *Rafty) switchPeerToUpOrDown(id, address string, up bool) {
	peerIndex := r.getPeerSliceIndex(address)
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Peers[peerIndex].id == "" {
		return
	}

	if up {
		if r.Peers[peerIndex].id == id && r.Peers[peerIndex].address.String() == address && r.Peers[peerIndex].isDown {
			r.Logger.Info().Msgf("Peer %s / %s is back up", address, id)
			r.Peers[peerIndex].isDown = false
		}
		return
	}

	if r.Peers[peerIndex].id == id && r.Peers[peerIndex].address.String() == address && !r.Peers[peerIndex].isDown {
		r.Logger.Info().Msgf("Peer %s / %s is going down", address, id)
		r.Peers[peerIndex].client = nil
		r.Peers[peerIndex].rclient = nil
		r.Peers[peerIndex].isDown = true
	}
}

// switchState permits to switch to the mentionned state
// and print a nice message if needed
func (r *Rafty) switchState(state State, niceMessage bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State == state {
		return
	}

	r.State = state
	if niceMessage {
		switch r.State {
		case Follower:
			r.Logger.Info().Msgf("Stepping down as %s for term %d", r.State.String(), r.CurrentTerm)
		case Candidate:
			r.Logger.Info().Msgf("Stepping up as %s for term %d", r.State.String(), r.CurrentTerm)
		case Leader:
			r.Logger.Info().Msgf("Stepping up as %s for term %d", r.State.String(), r.CurrentTerm)
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
	defer r.mu.Unlock()
	if r.Peers[peerIndex].client == nil {
		conn, err := grpc.Dial(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(kacp),
		)

		if err != nil {
			r.Logger.Err(err).Msgf("Fail to connect to peer %s", address)
			return
		}
		r.Peers[peerIndex].client = conn
		r.Peers[peerIndex].rclient = grpcrequests.NewRaftyClient(conn)
	}
}

// disconnectToPeers permits to disconnect to all grpc servers
func (r *Rafty) disconnectToPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.Peers {
		if peer.client != nil {
			err := peer.client.Close()
			if err != nil {
				r.Logger.Err(err).Msgf("Fail to close connection to peer %s", peer.id)
				return
			}
			peer.client = nil
			peer.rclient = nil
		}
	}
}
