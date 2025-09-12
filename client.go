package rafty

import (
	"bytes"
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

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
