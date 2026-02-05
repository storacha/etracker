package metrics

import (
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var log = logging.Logger("metrics")

var (
	// TrackedBatchesPerNode counts the number of egress batches tracked per node
	TrackedBatchesPerNode metric.Int64Counter

	// ConsolidatedBytesPerNode counts the consolidated bytes per node
	ConsolidatedBytesPerNode metric.Int64Counter

	// UnprocessedBatches keeps track of the total number of batches pending consolidation
	UnprocessedBatches metric.Int64UpDownCounter
)

// Init initializes the OpenTelemetry metrics with Prometheus exporter
func Init() error {
	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	// Create a MeterProvider with the Prometheus exporter
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))

	// Set the global MeterProvider
	otel.SetMeterProvider(provider)

	// Get a meter
	meter := provider.Meter("github.com/storacha/etracker")

	// Create counters
	TrackedBatchesPerNode, err = meter.Int64Counter(
		"etracker_tracked_batches_total",
		metric.WithDescription("Total number of egress batches tracked per node"),
	)
	if err != nil {
		return fmt.Errorf("failed to create TrackedBatchesPerNode counter: %w", err)
	}

	ConsolidatedBytesPerNode, err = meter.Int64Counter(
		"etracker_consolidated_bytes_total",
		metric.WithDescription("Total consolidated bytes per node"),
	)
	if err != nil {
		return fmt.Errorf("failed to create ConsolidatedBytesPerNode counter: %w", err)
	}

	UnprocessedBatches, err = meter.Int64UpDownCounter(
		"etracker_unprocessed_batches_total",
		metric.WithDescription("Total number of unprocessed batches"),
	)
	if err != nil {
		return fmt.Errorf("failed to create UnprocessedBatches counter: %w", err)
	}

	log.Info("OpenTelemetry metrics initialized with Prometheus exporter")
	return nil
}
