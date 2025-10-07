package rafty

import (
	"github.com/Lord-Y/rafty/raftypb"
)

// LogKind represent the kind of the log
type LogKind uint8

const (
	// logNoop is a log type used only by the leader
	// to keep the log index and term in sync with followers
	// when stepping up as leader
	LogNoop LogKind = iota

	// logConfiguration is a log type used between nodes
	// when configuration need to change
	LogConfiguration

	// logReplication is a log type used by clients to append log entries
	// on all nodes
	LogReplication

	// logCommanRead is a log type use by clients to fetch data from the leader
	LogCommandReadLeader

	// logCommandReadStale is a log type use by clients to fetch data from any nodes.
	// if the current node is not a leader you may fetch outdated data
	LogCommandReadStale
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

// LogEntry is hold requirements that will be used
// to store logs on disk
type LogEntry struct {
	FileFormat uint32
	Tombstone  uint32
	LogType    uint32
	Timestamp  uint32
	Term       uint64
	Index      uint64
	Command    []byte
}
