package rafty

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// newMetrics initialize Prometheus metrics for monitoring node.
func newMetrics(nodeId, namespace string) *metrics {
	z := &metrics{
		id: nodeId,
		down: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "rafty",
				Name:      "state_down",
				Help:      "Indicates current node state",
			},
			[]string{"node_id"},
		),
		readReplica: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "rafty",
				Name:      "state_read_replica",
				Help:      "Indicates current node state",
			},
			[]string{"node_id"},
		),
		follower: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "rafty",
				Name:      "state_follower",
				Help:      "Indicates current node state",
			},
			[]string{"node_id"},
		),
		candidate: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "rafty",
				Name:      "state_candidate",
				Help:      "Indicates current node state",
			},
			[]string{"node_id"},
		),
		leader: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "rafty",
				Name:      "state_leader",
				Help:      "Indicates current node state",
			},
			[]string{"node_id"},
		),
		snapshotSave: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "rafty",
			Name:      "snapshot_save_total_duration_seconds",
			Help:      "Indicates how much time it took to take a snapshot",
		},
			[]string{"node_id"},
		),
		installSnapshot: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "rafty",
			Name:      "install_snapshot_total_duration_seconds",
			Help:      "Indicates how much time it took to install a snapshot from rpc reauest",
		},
			[]string{"node_id"},
		),
	}

	// Register the metrics with the default Prometheus registry
	// Make sure to register them all, otherwise, no metrics will be found
	if prometheus.DefaultRegisterer != nil {
		prometheus.DefaultRegisterer.MustRegister(z.down)
		prometheus.DefaultRegisterer.MustRegister(z.readReplica)
		prometheus.DefaultRegisterer.MustRegister(z.follower)
		prometheus.DefaultRegisterer.MustRegister(z.candidate)
		prometheus.DefaultRegisterer.MustRegister(z.leader)

		prometheus.DefaultRegisterer.MustRegister(z.snapshotSave)
		prometheus.DefaultRegisterer.MustRegister(z.installSnapshot)
	}
	// Unregister golang default collectors
	_ = prometheus.Unregister(collectors.NewGoCollector())
	_ = prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return z
}

// setNodeStateGauge will set the current node gauge state with the provided value
func (m *metrics) setNodeStateGauge(state State) {
	// Always reset the default values
	m.down.With(prometheus.Labels{"node_id": m.id}).Set(0)
	m.readReplica.With(prometheus.Labels{"node_id": m.id}).Set(0)
	m.follower.With(prometheus.Labels{"node_id": m.id}).Set(0)
	m.candidate.With(prometheus.Labels{"node_id": m.id}).Set(0)
	m.leader.With(prometheus.Labels{"node_id": m.id}).Set(0)

	switch state {
	case ReadReplica:
		m.readReplica.With(prometheus.Labels{"node_id": m.id}).Set(1)

	case Follower:
		m.follower.With(prometheus.Labels{"node_id": m.id}).Set(1)

	case Candidate:
		m.candidate.With(prometheus.Labels{"node_id": m.id}).Set(1)

	case Leader:
		m.leader.With(prometheus.Labels{"node_id": m.id}).Set(1)

	default:
		m.down.With(prometheus.Labels{"node_id": m.id}).Set(1)
	}
}

// timeSince will set an histogram showing how much time it took to perform the provided operation
func (m *metrics) timeSince(opteration string, start time.Time) {
	elapsed := float64(time.Since(start)) / float64(time.Second)
	switch opteration {
	case "takeSnapshot":
		m.snapshotSave.With(prometheus.Labels{"node_id": m.id}).Observe(elapsed)
	case "installSnapshot":
		m.installSnapshot.With(prometheus.Labels{"node_id": m.id}).Observe(elapsed)
	}
}
