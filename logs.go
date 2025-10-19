package rafty

import (
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
	case LogCommandReadLeaderLease:
		return "logCommandReadLeaderLease"
	case LogCommandLinearizableRead:
		return "logCommandLinearizableRead"
	}
	return "logNoop"
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
