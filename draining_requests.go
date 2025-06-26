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
			case data.responseChan <- &raftypb.PreVoteResponse{PeerID: r.id, Granted: false, CurrentTerm: r.currentTerm.Load()}:
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
			case data.responseChan <- &raftypb.VoteResponse{PeerID: r.id, Granted: false, CurrentTerm: r.currentTerm.Load()}:
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
		case data := <-r.rpcAppendEntriesRequestChan:
			select {
			case data.responseChan <- &raftypb.AppendEntryResponse{Success: false}:
			//nolint staticcheck
			default:
			}
		//nolint staticcheck
		default:
			return
		}
	}
}
