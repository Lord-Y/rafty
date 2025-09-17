package rafty

// MembershipChange holds state related to membership management of the raft server.
type MembershipChange uint32

const (
	// Add is use to add a voting or read replica node in the configuration.
	// It will not yet participate in any requests as it need
	// later on to be promoted with Promote
	Add MembershipChange = iota

	// Promote is used when a new node caught up leader log entries.
	// Promoting this node will allow it to be fully part of the cluster.
	Promote

	// Demote is used when a node needs to shut down for maintenance.
	// It will still received log entries from the leader but won't be part of
	// the quorum or election campaign. If I'm the current node, I will step down
	Demote

	// Remove is use to remove node in the cluster after beeing demoted.
	Remove

	// ForceRemove is use to force remove node in the cluster.
	// Using this must be used with caution as it could break the cluster
	ForceRemove

	// LeaveOnTerminate is a boolean that allow the current node to completely remove itself
	// from the cluster before shutting down by sending a LeaveOnTerminate command to the leader.
	// It's usually used by read replicas nodes.
	LeaveOnTerminate
)
