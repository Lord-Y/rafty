package rafty

import (
	"fmt"
	"slices"

	"github.com/Lord-Y/rafty/raftypb"
)

// logKind represent the kind of the log
type logKind uint8

const (
	// logCommand is a log type used by clients
	logCommand logKind = iota

	// logConfiguration is a log type used between nodes
	// when configuration need to change
	logConfiguration
)

// logs hold qll requirements to manipulate logs
type logs struct {
	// rafty holds rafty config
	rafty *Rafty

	// log hold all logs entries
	log []*raftypb.LogEntry
}

// logOperationReadResponse will be used by logOperationRequest.responseChan
type logOperationReadResponse struct {
	// Kind is the log operation kind
	logs []*raftypb.LogEntry

	// total is the total current total of logs
	total int

	// err return an error if there is one
	err error
}

// logOperationReadResponse will be used by logOperationRequest.responseChan
type logOperationReadLastLogResponse struct {
	// Kind is the log operation kind
	logs []*raftypb.LogEntry

	// total is the total current total of logs
	total int

	// sendSnapshot tell us if it's better to send a snapshot
	// instead of catchup logs
	sendSnapshot bool

	// lastLogIndex is the last log index to use when sending the catchup entries
	lastLogIndex uint64

	// lastLogTerm is the last log term of the logs from lastLogIndex
	lastLogTerm uint64

	// err return an error if there is one
	err error
}

// logOperationWipeResponse will be used by logOperationRequest.responseChan
type logOperationWipeResponse struct {
	// total is the total current total of logs
	total int

	// err return an error if there is one
	err error
}

// logEntry is hold requirements that will be used
// to store logs on disk
type logEntry struct {
	FileFormat uint8  // 1 byte
	Tombstone  uint8  // 1 byte
	LogType    uint8  // 1 byte
	Timestamp  uint32 // 4 bytes
	Term       uint64 // 8 bytes
	Index      uint64 // 8 bytes
	Command    []byte
}

// newLogs instantiate rafty with default logs configuration
func (r *Rafty) newLogs() logs {
	return logs{
		rafty: r,
	}
}

// total will return the total number of logs
func (r *logs) total() logOperationReadResponse {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()
	return logOperationReadResponse{total: len(r.rafty.logs.log)}
}

// all will return all logs
func (r *logs) all() logOperationReadResponse {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	totalLogs := len(r.rafty.logs.log)
	response := logOperationReadResponse{}
	response.logs = make([]*raftypb.LogEntry, totalLogs)
	response.total = totalLogs
	copy(response.logs, r.rafty.logs.log)
	return response
}

// fromIndex will return a single log from specified index
func (r *logs) fromIndex(index uint64) logOperationReadResponse {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	response := logOperationReadResponse{}
	totalLogs := len(r.rafty.logs.log)
	if totalLogs > 0 && totalLogs > int(index) {
		response.logs = make([]*raftypb.LogEntry, 1)
		copy(response.logs, []*raftypb.LogEntry{r.rafty.logs.log[index]})
		response.total = 1
	} else {
		response.err = ErrIndexOutOfRange
	}
	return response
}

// fromLastLogParameters will return a slice of logs
// not greater than options.MaxAppendEntries variable
// with lastLogIndex and lastLogTerm
func (r *logs) fromLastLogParameters(lastLogIndex, lastLogTerm uint64, peerAddress, peerId string) (response logOperationReadLastLogResponse) {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	totalLogs := len(r.rafty.logs.log)
	var limit uint

	if totalLogs == 1 {
		response.logs = make([]*raftypb.LogEntry, 1)
		copy(response.logs, r.rafty.logs.log)
		response.total = len(response.logs)
		response.lastLogIndex = r.rafty.logs.log[response.total-1].Index
		response.lastLogTerm = r.rafty.logs.log[response.total-1].Term
		return
	}

	if lastLogIndex == 0 && lastLogTerm == 0 {
		limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.rafty.options.MaxAppendEntries), 0)
		response.logs = make([]*raftypb.LogEntry, limit)
		copy(response.logs, r.rafty.logs.log[0:limit])
		response.total = len(response.logs)
		response.lastLogIndex = lastLogIndex
		response.lastLogTerm = lastLogTerm
		return
	}

	if index := slices.IndexFunc(r.rafty.logs.log, func(p *raftypb.LogEntry) bool {
		return p.Index == lastLogIndex && p.Term == lastLogTerm
	}); index != -1 {
		limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.rafty.options.MaxAppendEntries), uint(index))
		for _, entry := range r.rafty.logs.log[index:limit] {
			if entry.Term <= lastLogTerm {
				response.logs = append(response.logs, entry)
			}
		}
		response.total = len(response.logs)
		if response.total > 0 {
			response.lastLogIndex = r.rafty.logs.log[response.total-1].Index
			response.lastLogTerm = r.rafty.logs.log[response.total-1].Term
		}
		return
	}
	// finding closest entry
	if index := slices.IndexFunc(r.rafty.logs.log, func(p *raftypb.LogEntry) bool {
		return p.Index > lastLogIndex
	}); index != -1 {
		limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.rafty.options.MaxAppendEntries), uint(index))
		r.rafty.Logger.Trace().
			Str("address", r.rafty.Address.String()).
			Str("id", r.rafty.id).
			Str("state", r.rafty.getState().String()).
			Str("term", fmt.Sprintf("%d", r.rafty.currentTerm.Load())).
			Str("request_lastLogTerm", fmt.Sprintf("%d", lastLogTerm)).
			Str("index", fmt.Sprintf("%d", index)).
			Str("limit", fmt.Sprintf("%d", limit)).
			Str("limitLastLogIndex", fmt.Sprintf("%d", r.rafty.logs.log[limit-1].Index)).
			Str("limitLastLogTerm", fmt.Sprintf("%d", r.rafty.logs.log[limit-1].Term)).
			Str("totalLogs", fmt.Sprintf("%d", totalLogs)).
			Str("peerAddress", peerAddress).
			Str("peerId", peerId).
			Msg("Catchup append entries closest request")

		// when we find the closest entry, we need to give back leader prev log index and term
		// somehow, using make cause panic then calculation the final
		// lastLogTerm and lastLogIndex response so we use for loop instead of copy
		// response.logs = make([]*raftypb.LogEntry, limit)
		// copy(response.logs, r.rafty.logs.log[index:limit])
		response.logs = append(response.logs, r.rafty.logs.log[index:limit]...)
		response.total = len(response.logs)
		if response.total > 0 {
			response.lastLogIndex = r.rafty.logs.log[response.total-1].Index
			response.lastLogTerm = r.rafty.logs.log[response.total-1].Term
		}
	}

	return response
}

// appendEntries will safely append entries to log.
// Entry index will be updated for later use.
// It will also set lastLogIndex and lastLogTerm
func (r *logs) appendEntries(entries []*raftypb.LogEntry) int {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	totalLogs := len(r.rafty.logs.log)
	for index, entry := range entries {
		entry.Index = uint64(totalLogs + index)
		r.rafty.logs.log = append(r.rafty.logs.log, entry)
	}
	totalLogs = len(r.rafty.logs.log)
	r.rafty.lastLogIndex.Store(uint64(totalLogs - 1))
	r.rafty.lastLogTerm.Store(uint64(r.rafty.logs.log[r.rafty.lastLogIndex.Load()].Term))
	return totalLogs
}

// wipeEntries will safely remove log entries on followers
// from provided range
func (r *logs) wipeEntries(from, to uint64) logOperationWipeResponse {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	response := logOperationWipeResponse{}
	totalLogs := len(r.rafty.logs.log)
	if totalLogs > int(from) && totalLogs > int(to) {
		r.rafty.logs.log = slices.Delete(r.rafty.logs.log, int(from), int(to))
		totalLogs = len(r.rafty.logs.log)
		r.rafty.lastLogIndex.Store(uint64(totalLogs - 1))
		r.rafty.lastLogTerm.Store(uint64(r.rafty.logs.log[r.rafty.lastLogIndex.Load()].Term))
		response.total = totalLogs
	} else {
		response.err = ErrIndexOutOfRange
	}
	return response
}

// applyConfigEntry will check if logType is a configuration logType
// and return new list of peers
func (r *logs) applyConfigEntry(entry *raftypb.LogEntry, currentPeers []peer) ([]peer, error) {
	switch entry.LogType {
	case uint32(logConfiguration):
		peers, err := decodePeers(entry.Command)
		if err != nil {
			return nil, err
		}

		for i := range currentPeers {
			if index := slices.IndexFunc(peers, func(p peer) bool {
				return p.address.String() == currentPeers[i].Address
			}); index == -1 {
				currentPeers = append(currentPeers, peers[i])
			}
		}
		return currentPeers, nil
	default:
	}
	return nil, nil
}
