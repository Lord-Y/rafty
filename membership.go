package rafty

import (
	"fmt"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
	"github.com/google/uuid"
)

// MembershipChange holds state related to membership management of the raft server.
type MembershipChange uint32

const (
	// Add is use to add a voting or read replica node in the configuration.
	// It will not yet participate in any requests as it need
	// later on to be promoted with Promote
	Add MembershipChange = iota

	// Promote is used when a new node caught up leader log entries.
	// Promoting this node will allow it to be fully part of the cluster.
	Promote

	// Demote is used when a node needs to shut down for maintenance.
	// It will still received log entries from the leader but won't be part of
	// the quorum or election campaign. If I'm the current node, I will step down
	Demote

	// Remove is use to remove node in the cluster after beeing demoted.
	Remove

	// ForceRemove is use to force remove node in the cluster.
	// Using this must be used with caution as it could break the cluster
	ForceRemove

	// LeaveOnTerminate is a boolean that allow the current node to completely remove itself
	// from the cluster before shutting down by sending a LeaveOnTerminate command to the leader.
	// It's usually used by read replicas nodes.
	LeaveOnTerminate
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

// handleSendMembershipChangeRequest allow the current node to manage
// membership change requests coming from other nodes or devops
func (r *leader) handleSendMembershipChangeRequest(data RPCRequest) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	start := time.Now()

	request := data.Request.(*raftypb.MembershipChangeRequest)
	response := &raftypb.MembershipChangeResponse{}
	rpcResponse := RPCResponse{
		Response: response,
	}

	defer func() {
		r.membershipChangeInProgress.Store(false)
		data.ResponseChan <- rpcResponse
	}()

	response.LeaderAddress = r.rafty.Address.String()
	response.LeaderId = r.rafty.id

	if r.membershipChangeInProgress.Load() {
		rpcResponse.Error = ErrMembershipChangeInProgress
		return
	}
	r.membershipChangeInProgress.Store(true)
	defer r.membershipChangeInProgress.Store(false)

	member := Peer{
		ID:          request.Id,
		Address:     request.Address,
		ReadReplica: request.ReadReplica,
		address:     getNetAddress(request.Address),
	}

	follower := &followerReplication{
		Peer:                member,
		rafty:               r.rafty,
		newEntryChan:        make(chan *onAppendEntriesRequest, 1),
		replicationStopChan: make(chan struct{}, 1),
	}

	action := MembershipChange(request.Action)
	switch action {
	case Add:
		response.Success, rpcResponse.Error = r.addNode(member, request, follower)
		if response.Success {
			response.Success, rpcResponse.Error = r.promoteNode(Promote, member, follower)
		}
	case Promote:
		response.Success, rpcResponse.Error = r.promoteNode(action, member, follower)
	case Demote:
		response.Success, rpcResponse.Error = r.demoteNode(action, member)
	case Remove, ForceRemove:
		response.Success, rpcResponse.Error = r.removeNode(action, member)
	case LeaveOnTerminate:
		response.Success, rpcResponse.Error = r.removeNode(action, member)
	}

	r.rafty.Logger.Debug().
		Str("address", r.rafty.Address.String()).
		Str("id", r.rafty.id).
		Str("state", r.rafty.getState().String()).
		Str("peerAddress", member.Address).
		Str("peerId", member.ID).
		Str("success", fmt.Sprintf("%t", response.Success)).
		Str("action", action.String()).
		Str("duration", time.Since(start).String()).
		Msgf("Membership change completed")
}

// addNode will setup configuration to add a new node during membership change
func (r *leader) addNode(member Peer, req *raftypb.MembershipChangeRequest, followerRepl *followerReplication) (success bool, err error) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	currentTerm := r.rafty.currentTerm.Load()
	peers, _ := r.rafty.getAllPeers()

	action := MembershipChange(req.Action)
	nextConfig, _ := r.nextConfiguration(action, peers, member)
	encodedPeers := encodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	r.rafty.updateEntriesIndex(entries)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  r.rafty.lastLogIndex.Load(),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 time.Second,
		membershipChangeInProgress: &r.membershipChangeInProgress,
		membershipChangeID:         member.ID,
	}

	buildResponse := &raftypb.AppendEntryResponse{
		LogNotFound:  true,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	if !r.rafty.isPartOfTheCluster(member) {
		_ = r.rafty.applyConfigEntry(entries[0])
		if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
			panic(err)
		}

		for _, follower := range r.followerReplication {
			if r.rafty.getState() == Leader && follower != nil && (!follower.replicationStopped.Load() || r.rafty.isRunning.Load()) {
				follower.newEntryChan <- request
			}
		}
	}

	if err = followerRepl.catchupNewMember(member, request, buildResponse); err != nil {
		return
	}
	return true, nil
}

// catchupNewMember is used by the leader to send catchup entries
// to the new peer.
// If error is returned, it must retry membership change process.
// If node nextIndex is great or equal to provided leader nextIndex
// the node is in sync with the leader
func (r *followerReplication) catchupNewMember(member Peer, request *onAppendEntriesRequest, response *raftypb.AppendEntryResponse) error {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	timeout := r.rafty.electionTimeout() * 10
	membershipTimer := time.NewTicker(timeout)
	leaderNextIndex := r.rafty.nextIndex.Load()
	leaderMatchIndex := r.rafty.matchIndex.Load()

	for round := range maxRound {
		lastContactDate := r.lastContactDate.Load()
		select {
		case <-r.rafty.quitCtx.Done():
			return ErrShutdown

		case <-membershipTimer.C:
			return ErrMembershipChangeNodeTooSlow

		default:
			client := r.rafty.connectionManager.getClient(member.Address, member.ID)
			if client != nil && r.rafty.getState() == Leader {
				r.rafty.Logger.Debug().
					Str("address", r.rafty.Address.String()).
					Str("id", r.rafty.id).
					Str("state", r.rafty.getState().String()).
					Str("term", fmt.Sprintf("%d", request.term)).
					Str("nextIndex", fmt.Sprintf("%d", r.rafty.nextIndex.Load())).
					Str("matchIndex", fmt.Sprintf("%d", r.rafty.matchIndex.Load())).
					Str("peerAddress", r.Address).
					Str("peerId", r.ID).
					Str("peerNextIndex", fmt.Sprintf("%d", leaderNextIndex)).
					Str("peerMatchIndex", fmt.Sprintf("%d", leaderMatchIndex)).
					Str("membershipChange", fmt.Sprintf("%t", request.membershipChangeInProgress.Load())).
					Str("round", fmt.Sprintf("%d", round)).
					Msgf("Begin catching up log entries for membership")

				r.sendCatchupAppendEntries(client, request, response)
			}

			if lastContactDate != r.lastContactDate.Load() {
				membershipTimer.Reset(timeout)
			}
			if leaderNextIndex <= r.nextIndex.Load() && request.membershipChangeCommitted.Load() {
				return nil
			}
		}
	}
	return ErrMembershipChangeNodeTooSlow
}

// promoteNode will promote node by add new log entry config for replication.
// The new node will be then added in the replication stream and will receive
// append entries like other nodes
func (r *leader) promoteNode(action MembershipChange, member Peer, follower *followerReplication) (success bool, err error) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	currentTerm := r.rafty.currentTerm.Load()
	peers, _ := r.rafty.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.rafty.id,
		Address: r.rafty.Address.String(),
	})

	nextConfig, _ := r.nextConfiguration(action, peers, member)
	encodedPeers := encodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	r.rafty.updateEntriesIndex(entries)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  r.rafty.lastLogIndex.Load(),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &r.membershipChangeInProgress,
		membershipChangeID:         member.ID,
	}

	_ = r.rafty.applyConfigEntry(entries[0])
	if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
		panic(err)
	}

	for _, follower := range r.followerReplication {
		if r.isReplicable(follower) {
			follower.newEntryChan <- request
		}
	}

	r.addReplication(follower, false)
	r.totalFollowers.Add(1)
	return true, nil
}

// demoteNode will demote node by add config into log replication process
// and will receive append entries like other nodes
func (r *leader) demoteNode(action MembershipChange, member Peer) (success bool, err error) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	currentTerm := r.rafty.currentTerm.Load()
	peers, _ := r.rafty.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.rafty.id,
		Address: r.rafty.Address.String(),
	})

	var nextConfig []Peer
	if nextConfig, err = r.nextConfiguration(action, peers, member); err != nil {
		return
	}
	encodedPeers := encodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	r.rafty.updateEntriesIndex(entries)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  r.rafty.lastLogIndex.Load(),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &r.membershipChangeInProgress,
		membershipChangeID:         member.ID,
	}

	_ = r.rafty.applyConfigEntry(entries[0])
	if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
		panic(err)
	}

	request.totalFollowers = r.totalFollowers.Load()
	for _, follower := range r.followerReplication {
		if r.isReplicable(follower) {
			follower.newEntryChan <- request
		}
	}

	if index := slices.IndexFunc(nextConfig, func(p Peer) bool {
		return p.Address == member.Address && p.ID == member.ID
	}); index != -1 {
		for _, follower := range r.followerReplication {
			if follower != nil && follower.ID == member.ID {
				follower.WaitToBePromoted = nextConfig[index].WaitToBePromoted
				follower.Decommissioning = nextConfig[index].Decommissioning
				break
			}
		}
	}

	// if it's myself
	if member.ID == r.rafty.id && member.Address == r.rafty.Address.String() {
		go r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
	}
	return true, nil
}

// removeNode will:
//
// - remove provided node in the configuration
//
// - remove node from replication process
//
// - if node to remove is the provided node will step down or shutdown
func (r *leader) removeNode(action MembershipChange, member Peer) (success bool, err error) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()

	currentTerm := r.rafty.currentTerm.Load()
	peers, _ := r.rafty.getPeers()
	// add myself, the current leader
	peers = append(peers, Peer{
		ID:      r.rafty.id,
		Address: r.rafty.Address.String(),
	})

	var nextConfig []Peer
	if nextConfig, err = r.nextConfiguration(action, peers, member); err != nil {
		return
	}
	encodedPeers := encodePeers(nextConfig)
	entries := []*raftypb.LogEntry{
		{
			LogType:   uint32(logConfiguration),
			Timestamp: uint32(time.Now().Unix()),
			Term:      currentTerm,
			Command:   encodedPeers,
		},
	}

	r.rafty.updateEntriesIndex(entries)
	request := &onAppendEntriesRequest{
		totalFollowers:             r.totalFollowers.Load(),
		quorum:                     uint64(r.rafty.quorum()),
		term:                       currentTerm,
		prevLogIndex:               r.rafty.lastLogIndex.Load(),
		prevLogTerm:                r.rafty.lastLogTerm.Load(),
		totalLogs:                  r.rafty.lastLogIndex.Load(),
		uuid:                       uuid.NewString(),
		commitIndex:                r.rafty.commitIndex.Load(),
		entries:                    entries,
		catchup:                    true,
		rpcTimeout:                 r.rafty.randomRPCTimeout(true),
		membershipChangeInProgress: &r.membershipChangeInProgress,
		membershipChangeID:         member.ID,
	}

	_ = r.rafty.applyConfigEntry(entries[0])
	if err := r.rafty.logStore.storeMetadata(r.rafty.buildMetadata()); err != nil {
		panic(err)
	}

	for _, follower := range r.followerReplication {
		if action == LeaveOnTerminate && follower != nil && follower.ID == member.ID {
			continue
		}
		if r.isReplicable(follower) {
			follower.newEntryChan <- request
		}
	}
	r.totalFollowers.Store(r.totalFollowers.Load() - 1)
	for _, follower := range r.followerReplication {
		if follower != nil && follower.ID == member.ID {
			follower.replicationStopChan <- struct{}{}
			r.stopReplication(follower)
			delete(r.followerReplication, follower.ID)
			break
		}
	}

	// if it's myself
	if !isPartOfTheCluster(nextConfig, member) {
		if r.rafty.options.ShutdownOnRemove {
			go r.rafty.stop()
			return true, nil
		}
		go r.rafty.switchState(Follower, stepDown, true, r.rafty.currentTerm.Load())
	}
	return true, nil
}

// nextConfiguration will create the next configuration config based on the action
// to perform. member is the peer to take action on
func (r *leader) nextConfiguration(action MembershipChange, current []Peer, member Peer) ([]Peer, error) {
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
func (r *leader) verifyConfiguration(peers []Peer) bool {
	var voters int
	for _, node := range peers {
		if !node.ReadReplica && !node.WaitToBePromoted && !node.Decommissioning {
			voters++
		}
	}
	return voters > 1 && voters >= r.rafty.quorum()
}
