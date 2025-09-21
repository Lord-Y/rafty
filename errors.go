package rafty

import "errors"

var (
	// ErrAppendEntriesToLeader is triggered when trying to append entries to a leader
	ErrAppendEntriesToLeader = errors.New("cannot send append entries to a leader")

	// ErrTermTooOld is triggered when the term of the peer is older than mine
	ErrTermTooOld = errors.New("peer term older than mine")

	// ErrNoLeader is triggered when there is no existing leader
	ErrNoLeader = errors.New("no leader")

	// ErrNotLeader is triggered when trying to perform an operation on a non leader
	ErrNotLeader = errors.New("not leader")

	// ErrShutdown is triggered when the node is shutting down
	ErrShutdown = errors.New("node is shutting down")

	// ErrLogNotFound is triggered when the provided log is not found on the current node
	ErrLogNotFound = errors.New("log not found")

	// ErrIndexOutOfRange is triggered when trying to fetch an index that does not exist
	ErrIndexOutOfRange = errors.New("index out of range")

	// ErrUnkownRPCType is triggered when trying to perform an rpc call with wrong parameter
	ErrUnkownRPCType = errors.New("unknown rpcType")

	// ErrTimeout is triggered when an operation timed out
	ErrTimeout = errors.New("operation timeout")

	// ErrUnkown is triggered when trying to perform an operation that does not exist
	ErrUnkown = errors.New("unknown error")

	// ErrLeadershipTransferInProgress is triggered when a node receive an append entry request
	// or a vote request
	ErrLeadershipTransferInProgress = errors.New("leadership transfer in progress")

	// ErrMembershipChangeNodeTooSlow is triggered by a timeout when a node is to slow to catch leader logs
	ErrMembershipChangeNodeTooSlow = errors.New("new node is too slow catching up leader logs")

	// ErrMembershipChangeInProgress is triggered by the leader when it already has a membership in progress
	ErrMembershipChangeInProgress = errors.New("membership change in progress")

	// ErrMembershipChangeNodeNotDemoted is triggered by the leader when remove a node without
	// demoting it first
	ErrMembershipChangeNodeNotDemoted = errors.New("node must be demoted before being removed")

	// ErrMembershipChangeNodeDemotionForbidden is triggered by the leader when trying to demote a node can
	// break cluster quorum
	ErrMembershipChangeNodeDemotionForbidden = errors.New("node cannot be demoted, breaking cluster")

	// ErrClusterNotBootstrapped is triggered by the leader when the bootstrap options is set
	// and a client is submitting a command
	ErrClusterNotBootstrapped = errors.New("cluster not bootstrapped")

	// ErrClusterAlreadyBootstrapped is triggered when trying to bootstrap the cluster again
	ErrClusterAlreadyBootstrapped = errors.New("cluster already bootstrapped")

	// ErrChecksumDataTooShort is triggered while decoding data with checksum
	ErrChecksumDataTooShort = errors.New("data to short for checksum")

	// ErrChecksumMistmatch is triggered while decoding data. It generally happen when
	// a file is corrupted
	ErrChecksumMistmatch = errors.New("CRC32 checksum mistmatch")

	// ErrKeyNotFound is triggered when trying to fetch a key that does not exist
	ErrKeyNotFound = errors.New("key not found")

	// ErrStoreClosed is triggered when trying to perform an operation
	// while the store is closed
	ErrStoreClosed = errors.New("store closed")

	// ErrDataDirRequired is triggered when data dir is missing
	ErrDataDirRequired = errors.New("data dir cannot be empty")

	// ErrNoSnapshotToTake is triggered when there is no snapshot to take
	ErrNoSnapshotToTake = errors.New("no snapshot to take")

	// ErrLogCommandNotAllowed is triggered when submitting a command that is not allowed
	ErrLogCommandNotAllowed = errors.New("log command not allowed")

	// ErrClient is triggered when failed to built grpc client
	ErrClient = errors.New("fail to get grpc client")
)
