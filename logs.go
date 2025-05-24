package rafty

import (
	"slices"

	"github.com/Lord-Y/rafty/raftypb"
)

// logOperationKind represent the operation
// that will be performed on logs
type logOperationKind uint8

// logKind represent the kind of the log
type logKind uint8

const (
	// logOperationGetAll allow us to fetch all logs
	logOperationGetAll logOperationKind = iota

	// logOperationGetFromIndex allow us to fetch all logs
	// from specified index
	logOperationGetFromIndex

	// logOperationGetFromLastLogParameters allow us to fetch all logs
	// lesser than MaxAppendEntries option
	// starting at lastLogIndex with lastLogTerm
	logOperationGetFromLastLogParameters

	// logOperationGetTotal will simply return total logs
	logOperationGetTotal

	// logOperationWrite allow us to append logs
	logOperationWrite

	// logOperationWipeFromRange allow us to wipe logs from specified range
	logOperationWipeFromRange

	// logCommand is a log type used by clients
	logCommand logKind = iota

	// logConfiguration is a log type used between nodes
	// when configuration need to change
	logConfiguration
)

// logs hold qll requirements to manipulate logs
type logs struct {
	// log hold all logs entries
	log []*raftypb.LogEntry

	// logOperationChan is the chan that will be used
	// to read or write logs safely in memory
	logOperationChan chan logOperationRequest

	// lastAppliedConfig is the index of the highest log entry configuration applied
	// to the current raft server
	// lastAppliedConfig *atomic.Uint64
}

// logOperationRequest is the request to send to logOperationChan
type logOperationRequest struct {
	// Kind is the log operation kind
	kind logOperationKind

	// request is the operation to perform
	request any

	// responseChan hold the response operation
	responseChan chan<- logOperationResponse
}

// logOperationResponse hold the response operation
type logOperationResponse struct {
	response any
}

// logOperationReadRequest will be used by logOperationRequest.request
type logOperationReadRequest struct {
	// index is the starting index of the logs
	index uint64
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

// logOperationReadLastLogRequest will be used by logOperationRequest.request
type logOperationReadLastLogRequest struct {
	// lastLogIndex is the last log index
	lastLogIndex uint64

	// lastLogTerm is the last log term of the logs from lastLogIndex
	lastLogTerm uint64

	// peerAddress is the address of the peer. Stand for debugging
	peerAddress string

	// peerId is the id of the peer. Stand for debugging
	peerId string
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

	// err return an error if there is one
	err error
}

// logOperationWriteRequest will be used by logOperationRequest.request
type logOperationWriteRequest struct {
	// logs are logs to append
	logs []*raftypb.LogEntry
}

// logOperationWriteResponse will be used by logOperationRequest.responseChan
type logOperationWriteResponse struct {
	// total is the total current total of logs
	total int
}

// logOperationWipeRequest will be used by logOperationRequest.request
type logOperationWipeRequest struct {
	// from is the starting index of the logs
	from uint64

	// from is the starting index of the logs
	to uint64
}

// logOperationWipeResponse will be used by logOperationRequest.responseChan
type logOperationWipeResponse struct {
	// err return an error if there is one
	err error

	// total is the total current total of logs
	total int
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
		logOperationChan: make(chan logOperationRequest),
	}
}

// total will return the total number of logs
func (r *logs) total() logOperationReadResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind:         logOperationGetTotal,
		request:      logOperationReadRequest{},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationReadResponse)
}

// all will return all logs
func (r *logs) all() logOperationReadResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind:         logOperationGetAll,
		request:      logOperationReadRequest{},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationReadResponse)
}

// fromIndex will return a single log from specified index
func (r *logs) fromIndex(index uint64) logOperationReadResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind:         logOperationGetFromIndex,
		request:      logOperationReadRequest{index: index},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationReadResponse)
}

// fromLastLogParameters will return a slice of logs
// not greater than options.MaxAppendEntries variable
// with lastLogIndex and lastLogTerm
func (r *logs) fromLastLogParameters(lastLogIndex, lastLogTerm uint64, peerAddress, peerId string) logOperationReadLastLogResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind: logOperationGetFromLastLogParameters,
		request: logOperationReadLastLogRequest{
			lastLogIndex: lastLogIndex,
			lastLogTerm:  lastLogTerm,
			peerAddress:  peerAddress,
			peerId:       peerId,
		},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationReadLastLogResponse)
}

// appendEntries will safely append entries to log.
// Entry index will be updated for later use.
// It will also set lastLogIndex and lastLogTerm
func (r *logs) appendEntries(entries []*raftypb.LogEntry) logOperationWriteResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind:         logOperationWrite,
		request:      logOperationWriteRequest{logs: entries},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationWriteResponse)
}

// wipeEntries will safely remove log entries on followers
// from provided range
func (r *logs) wipeEntries(from, to uint64) logOperationWipeResponse {
	logResponse := make(chan logOperationResponse, 1)
	logRequest := logOperationRequest{
		kind:         logOperationWipeFromRange,
		request:      logOperationWipeRequest{from: from, to: to},
		responseChan: logResponse,
	}
	r.logOperationChan <- logRequest
	responseLog := <-logResponse
	return responseLog.response.(logOperationWipeResponse)
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
