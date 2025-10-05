package rafty

import "github.com/Lord-Y/rafty/raftypb"

// drainPreVoteRequests will drain all remaining requests in the chan
func (r *Rafty) drainPreVoteRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining pre vote requests chan")

	for {
		select {
		case data := <-r.rpcPreVoteRequestChan:
			select {
			case data.ResponseChan <- RPCResponse{
				Response: &raftypb.PreVoteResponse{PeerId: r.id, Granted: false, CurrentTerm: r.currentTerm.Load()},
			}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainVoteRequests will drain all remaining requests in the chan
func (r *Rafty) drainVoteRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining vote requests chan")

	for {
		select {
		case data := <-r.rpcVoteRequestChan:
			select {
			case data.ResponseChan <- RPCResponse{
				Response: &raftypb.VoteResponse{PeerId: r.id, Granted: false, CurrentTerm: r.currentTerm.Load()},
			}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainAppendEntriesRequests will drain all remaining requests in the chan
func (r *Rafty) drainAppendEntriesRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining append entries chan")

	for {
		select {
		case data := <-r.rpcAppendEntriesReplicationRequestChan:
			select {
			case data.ResponseChan <- RPCResponse{
				Response: &raftypb.AppendEntryResponse{Success: false},
			}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainGetLeaderResult will drain all remaining requests in the chan
func (r *Rafty) drainGetLeaderResult() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining get leader result chan")

	for {
		select {
		case <-r.rpcClientGetLeaderChan:
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainSendAskNodeIDRequests will drain all remaining requests in the chan
func (r *Rafty) drainSendAskNodeIDRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining send ask node id requests chan")

	for {
		select {
		case <-r.rpcAskNodeIDChan:
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainBootstrapClusterRequests will drain all remaining requests in the chan
func (r *Rafty) drainBootstrapClusterRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining bootstrap cluster requests chan")

	for {
		select {
		case data := <-r.rpcBootstrapClusterRequestChan:
			select {
			case data.ResponseChan <- RPCResponse{
				Response: &raftypb.BootstrapClusterResponse{},
				Error:    ErrShutdown,
			}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}

// drainInstallSnapshotRequests will drain all remaining requests in the chan
func (r *Rafty) drainInstallSnapshotRequests() {
	r.Logger.Trace().
		Str("address", r.Address.String()).
		Str("id", r.id).
		Str("state", r.getState().String()).
		Msgf("Draining install snapshot requests chan")

	for {
		select {
		case data := <-r.rpcInstallSnapshotRequestChan:
			select {
			case data.ResponseChan <- RPCResponse{
				Response: &raftypb.InstallSnapshotResponse{},
				Error:    ErrShutdown,
			}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}
