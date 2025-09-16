package rafty

import "io"

// StateMachine is an interface allowing clients
// to interact with the raft cluster
type StateMachine interface {
	// Snapshot allow the client to take snapshots
	Snapshot(snapshotWriter io.Writer) error

	// Restore allow the client to restore a snapshot
	Restore(snapshotReader io.Reader) error

	// ApplyCommand allow the client to apply a command to the state machine.
	// Only bytes are returned as the command can be forwarded to the leader
	// if called on a follower.
	ApplyCommand(cmd []byte) ([]byte, error)
}
