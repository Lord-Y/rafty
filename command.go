package rafty

// CommandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type CommandKind uint32

const (
	// commandGet command allow us to fetch data from the cluster
	CommandGet CommandKind = iota

	// CommandSet command allow us to write data from the cluster
	CommandSet
)

// Command is the struct to use to interact with cluster data
type Command struct {
	// Kind represent the set of commands: get, set, del
	Kind CommandKind

	// Key is the name of the key
	Key string

	// Value is the value associated to the key
	Value string
}
