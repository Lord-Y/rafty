package rafty

import (
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// SubmitCommand allow clients to submit command to the leader.
// Forwarded command will be multiplied by 5
func (r *Rafty) SubmitCommand(timeout time.Duration, logKind logKind, command []byte) ([]byte, error) {
	if r.options.BootstrapCluster && !r.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
	}
	if timeout == 0 {
		timeout = time.Second
	}

	switch logKind {
	case LogReplication:
		return r.submitCommandWrite(timeout, command)
	case LogCommandReadLeader:
		return r.submitCommandReadLeader(timeout, command)
	case LogCommandReadStale:
		return r.submitCommandReadStale(timeout, command)
	}
	return nil, ErrLogCommandNotAllowed
}

// submitCommandWrite is used to submit a command that will be written to the log and replicated to the followers
func (r *Rafty) submitCommandWrite(timeout time.Duration, command []byte) ([]byte, error) {
	if r.getState() == Leader {
		responseChan := make(chan RPCResponse, 1)
		select {
		case r.rpcAppendEntriesRequestChan <- RPCRequest{
			RPCType: AppendEntriesRequest,
			Request: replicateLogConfig{
				logType:    LogReplication,
				command:    command,
				client:     true,
				clientChan: responseChan,
			},
			ResponseChan: responseChan,
		}:
		case <-time.After(timeout):
			return nil, ErrTimeout
		}

		select {
		case response := <-responseChan:
			return nil, response.Error

		case <-r.quitCtx.Done():
			return nil, ErrShutdown

		case <-time.After(timeout):
			return nil, ErrTimeout
		}
	}

	leader := r.getLeader()
	if leader == (leaderMap{}) {
		return nil, ErrNoLeader
	}

	if client := r.connectionManager.getClient(leader.address); client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*timeout)
		defer cancel()
		response, err := client.ForwardCommandToLeader(
			ctx,
			&raftypb.ForwardCommandToLeaderRequest{
				LogType: uint32(LogReplication),
				Command: command,
			},
		)
		if err != nil {
			r.Logger.Error().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("leaderAddress", leader.address).
				Str("leaderId", leader.id).
				Msgf("Fail to forward command to leader")
			return nil, err
		}
		return response.Data, err
	}
	return nil, ErrClient
}

// submitCommandReadLeader is used to submit a command that will be read from the state machine on the leader.
// This command will not be written to the log and replicated to the followers
func (r *Rafty) submitCommandReadLeader(timeout time.Duration, command []byte) ([]byte, error) {
	if r.getState() != Leader {
		return nil, ErrNoLeader
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)
	var (
		response []byte
		err      error
	)
	go func() {
		response, err = r.fsm.ApplyCommand(&LogEntry{LogType: uint32(LogCommandReadLeader), Command: command})
		done <- struct{}{}
	}()

	select {
	case <-done:
		return response, err

	case <-r.quitCtx.Done():
		return nil, ErrShutdown

	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// submitCommandReadStale is used to submit a command that will be read from the state machine on any node.
// This command will not be written to the log and replicated to the followers
func (r *Rafty) submitCommandReadStale(timeout time.Duration, command []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)
	var (
		response []byte
		err      error
	)
	go func() {
		response, err = r.fsm.ApplyCommand(&LogEntry{LogType: uint32(LogCommandReadStale), Command: command})
		done <- struct{}{}
	}()

	// answer back to the client
	select {
	case <-done:
		return response, err

	case <-r.quitCtx.Done():
		return nil, ErrShutdown

	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// applyLogs apply the provided logs to the state machine
func (r *Rafty) applyLogs(logs applyLogs) ([]byte, error) {
	for _, entry := range logs.entries {
		if entry.Index <= r.lastApplied.Load() {
			continue
		}

		if entry.LogType != uint32(LogReplication) {
			continue
		}
		r.lastApplied.Store(entry.Index)
		return r.fsm.ApplyCommand(makeLogEntry(entry)[0])
	}
	return nil, nil
}

// BootstrapCluster is used by the current node
// to bootstrap the cluster with all initial nodes.
// This should be only call once and on one node.
// If already bootstrapped, and error will be returned
func (r *Rafty) BootstrapCluster(timeout time.Duration) error {
	responseChan := make(chan RPCResponse, 1)
	if timeout == 0 {
		timeout = time.Second
	}

	select {
	case r.rpcBootstrapClusterRequestChan <- RPCRequest{
		RPCType:      BootstrapClusterRequest,
		Request:      &raftypb.BootstrapClusterRequest{},
		ResponseChan: responseChan,
	}:
	case <-time.After(timeout):
		return ErrTimeout
	}

	select {
	case response := <-responseChan:
		return response.Error

	case <-r.quitCtx.Done():
		return ErrShutdown

	case <-time.After(timeout):
		return ErrTimeout
	}
}

// AddMember is used by the current node to add the provided member the cluster.
// An error will be returned if any
func (r *Rafty) AddMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Add, address, id, readReplica)
}

// PromoteMember is used by the current node to promote the provided member the cluster.
// An error will be returned if any
func (r *Rafty) PromoteMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Promote, address, id, readReplica)
}

// DemoteMember is used by the current node to demote the provided member the cluster.
// An error will be returned if any
func (r *Rafty) DemoteMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Demote, address, id, readReplica)
}

// RemoveMember is used by the current node to remove the provided member the cluster.
// An error will be returned if any
func (r *Rafty) RemoveMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Remove, address, id, readReplica)
}

// ForceRemoveMember is used by the current node to force remove the provided member the cluster.
// An error will be returned if any
func (r *Rafty) ForceRemoveMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, ForceRemove, address, id, readReplica)
}

// LeaveOnTerminateMember is used by the current node to remove itself from the cluster the cluster.
// An error will be returned if any
func (r *Rafty) LeaveOnTerminateMember(timeout time.Duration, address, id string, readReplica bool) error {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, LeaveOnTerminate, address, id, readReplica)
}

// manageMembers is a private func that handle all membership changes
func (r *Rafty) manageMembers(timeout time.Duration, action MembershipChange, address, id string, readReplica bool) error {
	member := Peer{
		ID:          id,
		Address:     address,
		ReadReplica: readReplica,
		address:     getNetAddress(address),
	}

	if r.getState() == Leader {
		responseChan := make(chan RPCResponse, 1)

		select {
		case r.rpcAppendEntriesRequestChan <- RPCRequest{
			RPCType: AppendEntriesRequest,
			Request: replicateLogConfig{
				logType:    LogConfiguration,
				client:     true,
				clientChan: responseChan,
				membershipChange: struct {
					action MembershipChange
					member Peer
				}{
					member: member,
					action: action,
				},
			},
			ResponseChan: responseChan,
		}:
		case <-time.After(timeout):
			return ErrTimeout
		}

		select {
		case response := <-responseChan:
			return response.Error

		case <-r.quitCtx.Done():
			return ErrShutdown

		case <-time.After(timeout):
			return ErrTimeout
		}
	}

	leader := r.getLeader()
	if leader == (leaderMap{}) {
		return ErrNoLeader
	}

	if client := r.connectionManager.getClient(leader.address); client != nil {
		// we encode only the member when we need to forward the command.
		// on target server it will be decoded and the right list will be submitted.
		// Normally, membership changes shouldn't be forwarded but we support it
		encodePeer := EncodePeers([]Peer{member})
		ctx, cancel := context.WithTimeout(context.Background(), 5*timeout)
		defer cancel()
		if _, err := client.ForwardCommandToLeader(ctx, &raftypb.ForwardCommandToLeaderRequest{
			LogType:           uint32(LogConfiguration),
			MembershipAction:  uint32(action),
			MembershipTimeout: uint32(4 * timeout),
			Command:           encodePeer,
		}); err != nil {
			r.Logger.Error().Err(err).
				Str("address", r.Address.String()).
				Str("id", r.id).
				Str("leaderAddress", leader.address).
				Str("leaderId", leader.id).
				Msgf("Fail to forward command to leader")
			return err
		}
		return nil
	}
	return ErrClient
}

// Status return the current status of the node
func (r *Rafty) Status() Status {
	configuration, _ := r.getAllPeers()
	return Status{
		State:                  r.getState(),
		Leader:                 r.getLeader(),
		CurrentTerm:            r.currentTerm.Load(),
		CommitIndex:            r.commitIndex.Load(),
		LastApplied:            r.lastApplied.Load(),
		LastLogIndex:           r.lastLogIndex.Load(),
		LastLogTerm:            r.lastLogTerm.Load(),
		LastAppliedConfigIndex: r.lastAppliedConfigIndex.Load(),
		LastAppliedConfigTerm:  r.lastAppliedConfigTerm.Load(),
		Configuration:          Configuration{ServerMembers: configuration},
	}
}

// IsBootstrapped return true if the cluster has been bootstrapped.
// It only applied when options.BootstrapCluster is set to true
func (r *Rafty) IsBootstrapped() bool {
	return r.isBootstrapped.Load()
}

// IsLeader return true if the current node is the leader
func (r *Rafty) IsLeader() bool {
	return r.getState() == Leader
}

// IsLeader return true if the current node is the leader
func (r *Rafty) Leader() (bool, string, string) {
	leader := r.getLeader()
	var lid, lad string
	switch {
	case r.getState() == Leader:
		lid, lad = r.id, r.Address.String()
		return true, lad, lid
	default:
		lid, lad = leader.id, leader.address
		return lid != "" && lad != "", lad, lid
	}
}

// IsRunning return a boolean tell if the node is running
// and ready to perform its duty
func (r *Rafty) IsRunning() bool {
	return r.isRunning.Load()
}
