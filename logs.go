package rafty

import (
	"fmt"
	"slices"

	"github.com/Lord-Y/rafty/raftypb"
)

// logKind represent the kind of the log
type logKind uint8

const (
	// logNoop is a log type used only by the leader
	// to keep the log index and term in sync with followers
	// when stepping up as leader
	logNoop logKind = iota

	// logCommand is a log type used by clients
	logCommand

	// logConfiguration is a log type used between nodes
	// when configuration need to change
	logConfiguration
)

// logs hold all requirements to manipulate logs
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
		log:   nil,
	}
}

// makeLogEntries will convert slice of protobuf entries to slice of log entries
func makeLogEntries(entries []*raftypb.LogEntry) (logs []*logEntry) {
	for _, entry := range entries {
		logs = append(logs, &logEntry{
			FileFormat: uint8(entry.FileFormat),
			Tombstone:  uint8(entry.Tombstone),
			LogType:    uint8(entry.LogType),
			Timestamp:  entry.Timestamp,
			Term:       entry.Term,
			Index:      entry.Index,
			Command:    entry.Command,
		})
	}
	return
}

// makeLogEntry will convert protobuf entry to slice of protobuf entries
func makeLogEntry(entry *raftypb.LogEntry) (logs []*logEntry) {
	return makeLogEntries([]*raftypb.LogEntry{entry})
}

// makeProtobufLogEntries will convert slice of log entries to slice of protobuf entries
func makeProtobufLogEntries(entries []*logEntry) (logs []*raftypb.LogEntry) {
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
func makeProtobufLogEntry(entry *logEntry) (logs []*raftypb.LogEntry) {
	return makeProtobufLogEntries([]*logEntry{entry})
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
	case uint32(logConfiguration):
		if entry.Index > r.lastAppliedConfigIndex.Load() || entry.Term > r.lastAppliedConfigTerm.Load() {
			peers, err := decodePeers(entry.Command)
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

// updateEntriesIndex will update entries index monotically.
// lastLogIndex and lastLogTerm will also be updated accordingly
func (r *Rafty) updateEntriesIndex(entries []*raftypb.LogEntry) {
	r.wg.Add(1)
	defer r.wg.Done()
	r.mu.Lock()
	defer r.mu.Unlock()

	lastLogIndex, lastLogTerm := uint64(0), uint64(0)
	for _, entry := range entries {
		lastLogIndex = r.lastLogIndex.Load() + 1
		entry.Index = lastLogIndex
		lastLogTerm = entry.Term
		r.lastLogIndex.Store(lastLogIndex)
		r.lastLogTerm.Store(lastLogTerm)
	}
}
