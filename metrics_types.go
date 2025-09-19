package rafty

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds Prometheus metrics for monitoring the Rafty node.
type metrics struct {
	// id is the node ID used as a label for the metrics
	id string

	// down is a gauge that indicates the current node state
	down *prometheus.GaugeVec

	// readReplica is a gauge that indicates the current node state
	readReplica *prometheus.GaugeVec

	// follower is a gauge that indicates the current node state
	follower *prometheus.GaugeVec

	// candidate is a gauge that indicates the current node state
	candidate *prometheus.GaugeVec

	// leader is a gauge that indicates the current node state
	leader *prometheus.GaugeVec

	// snapshotSave is an histogram that indicates how much time it took to take a snapshot
	snapshotSave *prometheus.HistogramVec

	// installSnapshot is an histogram that indicates how much time it took to install a snapshot from rpc reauest
	installSnapshot *prometheus.HistogramVec
}
