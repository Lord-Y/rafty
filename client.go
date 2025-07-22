package rafty

import (
	"bytes"
	"context"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

func (r *Rafty) SubmitCommand(command Command) ([]byte, error) {
	buffer := new(bytes.Buffer)
	if err := encodeCommand(command, buffer); err != nil {
		return nil, err
	}
	return r.submitCommand(buffer.Bytes())
}

func (r *Rafty) submitCommand(command []byte) ([]byte, error) {
	if r.options.BootstrapCluster && !r.isBootstrapped.Load() {
		return nil, ErrClusterNotBootstrapped
	}
	cmd, err := decodeCommand(command)
	if err != nil {
		return nil, err
	}
	switch cmd.Kind {
	case CommandGet:
	case CommandSet:
		if r.getState() == Leader {
			responseChan := make(chan appendEntriesResponse, 1)
			r.triggerAppendEntriesChan <- triggerAppendEntries{command: command, responseChan: responseChan}

			// answer back to the client
			response := <-responseChan
			return response.Data, response.Error
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
