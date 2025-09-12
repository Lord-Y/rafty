package rafty

import (
	"bytes"
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// Status represent the current status of the node
type Status struct {
	// State is the current state of the node
	State State

	// Leader is the current leader of the cluster, if any
	Leader leaderMap

	// currentTerm is the current term of the node
	CurrentTerm uint64

	// CommittedIndex is the last committed index of the node
	CommitIndex uint64

	// LastApplied is the last applied index of the node
	LastApplied uint64

	// LastLogIndex is the last log index of the node
	LastLogIndex uint64

	// LastLogTerm is the last log term of the node
	LastLogTerm uint64

	// LastAppliedConfigIndex is the index of the last applied configuration
	LastAppliedConfigIndex uint64

	// LastAppliedConfigTerm is the term of the last applied configuration
	LastAppliedConfigTerm uint64

	// Configuration is the current configuration of the cluster
	Configuration Configuration
}

// SubmitCommand allow clients to submit command to the leader
func (r *Rafty) SubmitCommand(command Command) ([]byte, error) {
	buffer := new(bytes.Buffer)
	if err := EncodeCommand(command, buffer); err != nil {
		return nil, err
	}
	return r.submitCommand(buffer.Bytes())
}

// submitCommand is a private func that allow clients to submit command
// to the leader. It's used by SubmitCommand
func (r *Rafty) submitCommand(command []byte) ([]byte, error) {
	if r.options.BootstrapCluster && !r.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
	}
	cmd, err := DecodeCommand(command)
	if err != nil {
		return nil, err
	}
	switch cmd.Kind {
	case CommandGet:
	case CommandSet:
		if r.getState() == Leader {
			responseChan := make(chan appendEntriesResponse, 1)
			select {
			case r.triggerAppendEntriesChan <- triggerAppendEntries{command: command, responseChan: responseChan}:
			case <-time.After(500 * time.Millisecond):
				return nil, ErrTimeoutSendingRequest
			}

			// answer back to the client
			select {
			case response := <-responseChan:
				return response.Data, response.Error

			case <-time.After(500 * time.Millisecond):
				return nil, ErrTimeoutSendingRequest
			}
		} else {
			leader := r.getLeader()
			if leader == (leaderMap{}) {
				return nil, ErrNoLeader
			}

			client := r.connectionManager.getClient(leader.address, leader.id)
			if client != nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
		}
	}
	return nil, ErrCommandNotFound
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
