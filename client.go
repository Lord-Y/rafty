package rafty

import (
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// SubmitCommand allow clients to submit command to the leader
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
		responseChan := make(chan appendEntriesResponse, 1)
		select {
		case r.triggerAppendEntriesChan <- triggerAppendEntries{command: command, responseChan: responseChan}:
		case <-time.After(timeout):
			return nil, ErrTimeoutSendingRequest
		}

		// answer back to the client
		select {
		case response := <-responseChan:
			return response.Data, response.Error

		case <-r.quitCtx.Done():
			return nil, ErrShutdown

		case <-time.After(timeout):
			return nil, ErrTimeoutSendingRequest
		}
	}

	leader := r.getLeader()
	if leader == (leaderMap{}) {
		return nil, ErrNoLeader
	}

	if client := r.connectionManager.getClient(leader.address); client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*timeout)
		defer cancel()
		response, err := client.ForwardCommandToLeader(
			ctx,
			&raftypb.ForwardCommandToLeaderRequest{
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
	if r.getState() == Leader {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		done := make(chan struct{}, 1)
		var (
			response []byte
			err      error
		)
		go func() {
			response, err = r.fsm.ApplyCommand(command)
			done <- struct{}{}
		}()

		// answer back to the client
		select {
		case <-done:
			return response, err

		case <-r.quitCtx.Done():
			return nil, ErrShutdown

		case <-ctx.Done():
			return nil, ErrTimeoutSendingRequest
		}
	}

	return nil, ErrNoLeader
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
		response, err = r.fsm.ApplyCommand(command)
		done <- struct{}{}
	}()

	// answer back to the client
	select {
	case <-done:
		return response, err

	case <-r.quitCtx.Done():
		return nil, ErrShutdown

	case <-ctx.Done():
		return nil, ErrTimeoutSendingRequest
	}
}

// applyLogs apply the provided logs to the state machine
func (r *Rafty) applyLogs(logs applyLogs) ([]byte, error) {
	var (
		response []byte
		err      error
	)
	for _, entry := range logs.entries {
		if entry.Index <= r.lastApplied.Load() {
			continue
		}

		if entry.LogType != uint32(LogReplication) {
			continue
		}
		response, err = r.fsm.ApplyCommand(entry.Command)
		if err != nil {
			return nil, err
		}
	}
	return response, nil
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
		return ErrTimeoutSendingRequest
	}

	select {
	case response := <-responseChan:
		return response.Error

	case <-r.quitCtx.Done():
		return ErrShutdown

	case <-time.After(timeout):
		return ErrTimeoutSendingRequest
	}
}

// DemoteMember is used by the current node to demote the provided member the cluster. An error will be returned if any
func (r *Rafty) DemoteMember(timeout time.Duration, address, id string, readReplica bool) (*raftypb.MembershipChangeResponse, error) {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Demote, address, id, readReplica)
}

// RemoveMember is used by the current node to remove the provided member the cluster. An error will be returned if any
func (r *Rafty) RemoveMember(timeout time.Duration, address, id string, readReplica bool) (*raftypb.MembershipChangeResponse, error) {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, Remove, address, id, readReplica)
}

// ForceRemoveMember is used by the current node to force remove the provided member the cluster. An error will be returned if any
func (r *Rafty) ForceRemoveMember(timeout time.Duration, address, id string, readReplica bool) (*raftypb.MembershipChangeResponse, error) {
	if timeout == 0 {
		timeout = time.Second
	}

	return r.manageMembers(timeout, ForceRemove, address, id, readReplica)
}

// manageMembers is a private func that handle all membership changes
func (r *Rafty) manageMembers(timeout time.Duration, action MembershipChange, address, id string, readReplica bool) (*raftypb.MembershipChangeResponse, error) {
	responseChan := make(chan RPCResponse, 1)

	select {
	case r.rpcMembershipChangeRequestChan <- RPCRequest{
		RPCType: MembershipChangeRequest,
		Request: &raftypb.MembershipChangeRequest{
			Id:          id,
			Address:     address,
			ReadReplica: readReplica,
			Action:      uint32(action),
		},
		ResponseChan: responseChan,
	}:
	case <-time.After(timeout):
		return nil, ErrTimeoutSendingRequest
	}

	select {
	case response := <-responseChan:
		return response.Response.(*raftypb.MembershipChangeResponse), response.Error

	case <-r.quitCtx.Done():
		return nil, ErrShutdown

	case <-time.After(timeout):
		return nil, ErrTimeoutSendingRequest
	}
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
