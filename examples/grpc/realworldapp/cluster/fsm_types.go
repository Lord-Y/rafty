package cluster

import "github.com/Lord-Y/rafty"

// fsmState is a struct holding a set of configs required for fsm
type fsmState struct {
	// LogStore is the store holding the data
	logStore rafty.LogStore

	// memoryStore is only for user land management
	memoryStore memoryStore
}

// commandKind represent the command that will be applied to the state machine
// It can be only be Get, Set, Delete
type commandKind uint32

const (
	// kvCommandGet command allows us to fetch data from kv fsm store
	kvCommandGet commandKind = iota

	// kvCommandGetAll command allows us to fetch all data from kv fsm store
	kvCommandGetAll

	// kvCommandSet command allows us to write data from kv fsm store
	kvCommandSet

	// kvCommandDelete command allows us to delete data from kv fsm store
	kvCommandDelete

	// kvCommandGet command allows us to fetch data from users fsm store
	userCommandGet

	// userCommandGetAll command allows us to fetch all data from users fsm store
	userCommandGetAll

	// userCommandSet command allows us to write data from users fsm store
	userCommandSet

	// userCommandDelete command allows us to delete data from users fsm store
	userCommandDelete
)

// kvCommand is the struct to use to interact with cluster kv data
type kvCommand struct {
	// Kind represent the set of commands: get, set, del
	Kind commandKind

	// Key is the name of the key
	Key string

	// Value is the value associated to the key
	Value string
}

// userCommand is the struct to use to interact with cluster data
type userCommand struct {
	// Kind represent the set of commands: get, set, del
	Kind commandKind

	// Key is the name of the key
	Key string

	// Value is the value associated to the key
	Value string
}
