package rafty

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

	// LogCommandReadLeaderLease is a log type use by clients to fetch data from the leader.
	// This mode is susceptible to time drift or long GC pause.
	// Use this method only if you don't mind to potentially have stale data
	LogCommandReadLeaderLease

	// LogCommandLinearizableRead is a command that guarantees read data validity.
	// No stale read can happen in this mode.
	LogCommandLinearizableRead
)

// LogEntry is holds requirements that will be used
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
