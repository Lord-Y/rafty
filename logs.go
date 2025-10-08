package rafty

import (
	"fmt"
	"slices"
	"time"

	"github.com/Lord-Y/rafty/raftypb"
)

// String return a human readable state of the raft server
func (s LogKind) String() string {
	switch s {
	case LogConfiguration:
		return "logConfiguration"
	case LogReplication:
		return "logReplication"
	case LogCommandReadLeader:
		return "logCommandReadLeader"
	}
	return "logNoop"
}

// newLogs instantiate rafty with default logs configuration
func (r *Rafty) newLogs() logs {
	return logs{
		rafty: r,
		log:   nil,
	}
}

// makeNewLogEntry will make a new log entry based
// on the provided parameters
func makeNewLogEntry(term uint64, logType LogKind, command []byte) *LogEntry {
	return &LogEntry{
		LogType:   uint32(logType),
		Timestamp: uint32(time.Now().Unix()),
		Term:      term,
		Command:   command,
	}
}

// makeLogEntries will convert slice of protobuf entries to slice of log entries
func makeLogEntries(entries []*raftypb.LogEntry) (logs []*LogEntry) {
	for _, entry := range entries {
		logs = append(logs, &LogEntry{
			FileFormat: entry.FileFormat,
			Tombstone:  entry.Tombstone,
			LogType:    entry.LogType,
			Timestamp:  entry.Timestamp,
			Term:       entry.Term,
			Index:      entry.Index,
			Command:    entry.Command,
		})
	}
	return
}

// makeLogEntry will convert protobuf entry to slice of protobuf entries
func makeLogEntry(entry *raftypb.LogEntry) (logs []*LogEntry) {
	return makeLogEntries([]*raftypb.LogEntry{entry})
}

// makeProtobufLogEntries will convert slice of log entries to slice of protobuf entries
func makeProtobufLogEntries(entries []*LogEntry) (logs []*raftypb.LogEntry) {
	for _, entry := range entries {
		logs = append(logs, &raftypb.LogEntry{
			FileFormat: uint32(entry.FileFormat),
			Tombstone:  uint32(entry.Tombstone),
			LogType:    uint32(entry.LogType),
			Timestamp:  entry.Timestamp,
			Term:       entry.Term,
			Index:      entry.Index,
			Command:    entry.Command,
		})
	}
	return
}

// makeProtobufLogEntry will convert log entry to slice of log entries
func makeProtobufLogEntry(entry *LogEntry) (logs []*raftypb.LogEntry) {
	return makeProtobufLogEntries([]*LogEntry{entry})
}

// /!\ DO NOT USE MAKE AND COPY IN THE FOLLOWING FUNCTIONS
// THIS WILL CREATE NIL OBJECT AND GENERATE NIL POINTER
// EXCEPTION

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
	response.logs = r.rafty.logs.log
	response.total = totalLogs
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
		response.logs = []*raftypb.LogEntry{r.rafty.logs.log[index]}
		response.total = 1
		return response
	}
	response.err = ErrIndexOutOfRange
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

	if lastLogIndex == 0 && lastLogTerm == 0 {
		limit, response.sendSnapshot = calculateMaxRangeLogIndex(uint(totalLogs), uint(r.rafty.options.MaxAppendEntries), 0)
		response.logs = r.rafty.logs.log[0:limit]
		response.total = len(response.logs)
		response.lastLogIndex = r.rafty.logs.log[response.total-1].Index
		response.lastLogTerm = r.rafty.logs.log[response.total-1].Term
		return
	}

	if totalLogs == 1 {
		response.logs = r.rafty.logs.log
		response.total = len(response.logs)
		response.lastLogIndex = r.rafty.logs.log[response.total-1].Index
		response.lastLogTerm = r.rafty.logs.log[response.total-1].Term
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
func (r *logs) appendEntries(entries []*raftypb.LogEntry, restore bool) int {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	totalLogs := len(r.rafty.logs.log)
	for index, entry := range entries {
		if !restore {
			entry.Index = uint64(totalLogs + index)
		}
		r.rafty.logs.log = append(r.rafty.logs.log, entry)
	}
	totalLogs = len(r.rafty.logs.log)
	if totalLogs > 0 {
		r.rafty.lastLogIndex.Store(uint64(totalLogs - 1))
		r.rafty.lastLogTerm.Store(r.rafty.logs.log[r.rafty.lastLogIndex.Load()].Term)
	}
	return totalLogs
}

// wipeEntries will safely remove log entries on followers
// from provided range.
// When from and to both equal, all logs will be wiped out
func (r *logs) wipeEntries(from, to uint64) logOperationWipeResponse {
	r.rafty.wg.Add(1)
	defer r.rafty.wg.Done()
	r.rafty.mu.Lock()
	defer r.rafty.mu.Unlock()

	response := logOperationWipeResponse{}
	totalLogs := len(r.rafty.logs.log)
	switch {
	case from == to:
		r.rafty.logs.log = nil
	case totalLogs > int(from) && totalLogs > int(to):
		r.rafty.logs.log = slices.Delete(r.rafty.logs.log, int(from), int(to))
		totalLogs = len(r.rafty.logs.log)
		r.rafty.lastLogIndex.Store(uint64(totalLogs - 1))
		r.rafty.lastLogTerm.Store(uint64(r.rafty.logs.log[r.rafty.lastLogIndex.Load()].Term))
		response.total = totalLogs
	default:
		response.err = ErrIndexOutOfRange
	}
	response.total = len(r.rafty.logs.log)
	return response
}

// applyConfigEntry will check if logType is a configuration logType
// and add new peer into configuration
func (r *Rafty) applyConfigEntry(entry *raftypb.LogEntry) error {
	r.wg.Add(1)
	defer r.wg.Done()

	switch entry.LogType {
	case uint32(LogConfiguration):
		if entry.Index > r.lastAppliedConfigIndex.Load() || entry.Term > r.lastAppliedConfigTerm.Load() {
			peers, err := DecodePeers(entry.Command)
			if err != nil {
				return err
			}

			r.updateServerMembers(peers)
			// if I have been removed from the cluster
			if !isPartOfTheCluster(peers, Peer{Address: r.Address.String(), ID: r.id, address: getNetAddress(r.Address.String())}) && r.options.ShutdownOnRemove {
				r.shutdownOnRemove.Store(true)
			}

			r.lastAppliedConfigIndex.Store(entry.Index)
			r.lastAppliedConfigTerm.Store(entry.Term)
			if r.options.BootstrapCluster && !r.isBootstrapped.Load() {
				r.isBootstrapped.Store(true)
				r.timer.Reset(r.randomElectionTimeout())
			}
		}
		return nil
	default:
	}
	return nil
}
