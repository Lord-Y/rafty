package rafty

import (
	"slices"
)

// String return a human readable membership state of the raft server
func (s MembershipChange) String() string {
	switch s {
	case Promote:
		return "promote"
	case Demote:
		return "demote"
	case Remove:
		return "remove"
	case ForceRemove:
		return "forceRemove"
	case LeaveOnTerminate:
		return "leaveOnTerminate"
	}
	return "add"
}

// validateSendMembershipChangeRequest will validate membership
// operation that must be done
func (r *Rafty) validateSendMembershipChangeRequest(action MembershipChange, member Peer) ([]byte, error) {

	switch action {
	case Add:
		return r.addNode(action, member)
	case Promote:
		return r.promoteNode(action, member)
	case Demote:
		return r.demoteNode(action, member)
	case Remove, ForceRemove:
		return r.removeNode(action, member)
	case LeaveOnTerminate:
		return r.removeNode(action, member)
	}

	return nil, ErrUnkown
}

// addNode will setup configuration to add a new node during membership change
func (r *Rafty) addNode(action MembershipChange, member Peer) ([]byte, error) {
	peers, _ := r.getAllPeers()
	nextConfig, _ := r.nextConfiguration(action, peers, member)

	return EncodePeers(nextConfig), nil
}

// promoteNode will promote node by add new log entry config for replication.
// The new node will be then added in the replication stream and will receive
// append entries like other nodes
func (r *Rafty) promoteNode(action MembershipChange, member Peer) ([]byte, error) {
	peers, _ := r.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.id,
		Address: r.Address.String(),
	})
	nextConfig, _ := r.nextConfiguration(action, peers, member)

	return EncodePeers(nextConfig), nil
}

// demoteNode will demote node by add config into log replication process
// and will receive append entries like other nodes
func (r *Rafty) demoteNode(action MembershipChange, member Peer) ([]byte, error) {
	peers, _ := r.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.id,
		Address: r.Address.String(),
	})

	nextConfig, err := r.nextConfiguration(action, peers, member)
	if err != nil {
		return nil, err
	}

	return EncodePeers(nextConfig), nil
}

// removeNode will:
//
// - remove provided node in the configuration
//
// - remove node from replication process
//
// - if node to remove is the provided node will step down or shutdown
func (r *Rafty) removeNode(action MembershipChange, member Peer) ([]byte, error) {
	peers, _ := r.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.id,
		Address: r.Address.String(),
	})

	nextConfig, err := r.nextConfiguration(action, peers, member)
	if err != nil {
		return nil, err
	}

	return EncodePeers(nextConfig), nil
}

// nextConfiguration will create the next configuration config based on the action
// to perform. member is the peer to take action on
func (r *Rafty) nextConfiguration(action MembershipChange, current []Peer, member Peer) ([]Peer, error) {
	switch action {
	case Add:
		if index := slices.IndexFunc(current, func(p Peer) bool {
			return p.Address == member.Address && p.ID == member.ID
		}); index == -1 {
			member.WaitToBePromoted = true
			member.Decommissioning = false
			current = append(current, member)
		}

	case Promote:
		if index := slices.IndexFunc(current, func(p Peer) bool {
			return p.Address == member.Address && p.ID == member.ID
		}); index != -1 {
			current[index].WaitToBePromoted = false
			current[index].Decommissioning = false
		}

	case Demote:
		if index := slices.IndexFunc(current, func(p Peer) bool {
			return p.Address == member.Address && p.ID == member.ID
		}); index != -1 {
			current[index].WaitToBePromoted = false
			current[index].Decommissioning = true
		}
		if !r.verifyConfiguration(current) {
			return nil, ErrMembershipChangeNodeDemotionForbidden
		}

	case Remove:
		if index := slices.IndexFunc(current, func(p Peer) bool {
			return p.Address == member.Address && p.ID == member.ID
		}); index != -1 {
			if !current[index].WaitToBePromoted && !current[index].Decommissioning {
				return nil, ErrMembershipChangeNodeNotDemoted
			}
		}

		current = slices.DeleteFunc(current, func(p Peer) bool {
			return p.Address == member.Address
		})

	case ForceRemove:
		current = slices.DeleteFunc(current, func(p Peer) bool {
			return p.Address == member.Address
		})

	case LeaveOnTerminate:
		current = slices.DeleteFunc(current, func(p Peer) bool {
			return p.Address == member.Address
		})
	}
	return current, nil
}

// verifyConfiguration will verify the new configuration by checking if we have
// enough voters and make sure that any operation won't break the cluster.
func (r *Rafty) verifyConfiguration(peers []Peer) bool {
	var voters int
	for _, node := range peers {
		if node.IsVoter && !node.WaitToBePromoted && !node.Decommissioning {
			voters++
		}
	}
	return voters > 1 && voters >= r.quorum()
}
