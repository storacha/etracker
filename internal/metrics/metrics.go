package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TrackedBatchesPerNode counts the number of egress batches tracked per node
	TrackedBatchesPerNode = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etracker_tracked_batches_total",
			Help: "Total number of egress batches tracked per node",
		},
		[]string{"node_id"},
	)

	// ConsolidatedBytesPerNode counts the consolidated bytes per node
	ConsolidatedBytesPerNode = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "etracker_consolidated_bytes_total",
			Help: "Total consolidated bytes per node",
		},
		[]string{"node_id"},
	)
)
