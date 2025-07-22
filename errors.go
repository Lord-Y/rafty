package rafty

import "errors"

var (
	ErrAppendEntriesToLeader                 = errors.New("cannot send append entries to a leader")
	ErrTermTooOld                            = errors.New("peer term older than mine")
	ErrNoLeader                              = errors.New("no leader")
	ErrNotLeader                             = errors.New("not leader")
	ErrCommandNotFound                       = errors.New("commandNotFound")
	ErrShutdown                              = errors.New("node is shutting down")
	ErrLogNotFound                           = errors.New("log not found")
	ErrLogMismatch                           = errors.New("log mismatch")
	ErrLogNotUpToDateEnough                  = errors.New("log not up to date enough")
	ErrIndexOutOfRange                       = errors.New("index out of range")
	errUnkownRPCType                         = errors.New("unknown rpcType")
	errTimeoutSendingRequest                 = errors.New("timeout sending request")
	ErrUnkown                                = errors.New("unknown error")
	ErrLeadershipTransferInProgress          = errors.New("leadership transfer in progress")
	ErrMembershipChangeNodeTooSlow           = errors.New("new node is too slow catching up leader logs")
	ErrMembershipChangeInProgress            = errors.New("membership change in progress")
	ErrMembershipChangeNodeNotDemoted        = errors.New("node must be demoted before being removed")
	ErrMembershipChangeNodeDemotionForbidden = errors.New("node cannot be demoted, breaking cluster")
	ErrClusterNotBootstrapped                = errors.New("cluster not bootstrapped")
	ErrClusterAlreadyBootstrapped            = errors.New("cluster already bootstrapped")
)
