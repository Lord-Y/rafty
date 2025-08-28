package rafty

import "io"

// StateMachine is an interface allowing clients
// to interact with the raft cluster
type StateMachine interface {
	// Snapshot allow the client to take snapshots
	Snapshot(snapshotWriter io.Writer) error

	// Restore allow the client to restore a snapshot
	Restore(snapshotReader io.Reader) error
}
