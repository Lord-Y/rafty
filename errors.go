package rafty

import "errors"

var (
	ErrAppendEntriesToLeader = errors.New("cannot send append entries to a leader")
	ErrTermTooOld            = errors.New("peer term older than mine")
	ErrNoLeader              = errors.New("noLeader")
	ErrCommandNotFound       = errors.New("commandNotFound")
	ErrShutdown              = errors.New("node is shutting down")
	ErrLogNotFound           = errors.New("log not found")
	ErrLogMismatch           = errors.New("log mismatch")
	ErrIndexOutOfRange       = errors.New("index out of range")
	errUnkownRPCType         = errors.New("unknown rpcType")
	errTimeoutSendingRequest = errors.New("timeout sending request")
)
