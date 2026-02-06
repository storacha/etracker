package metrics

import (
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

var log = logging.Logger("metrics")

var (
	// TrackedBatchesPerNode counts the number of egress batches tracked per node
	TrackedBatchesPerNode metric.Int64Counter

	// ConsolidatedBytesPerNode counts the consolidated bytes per node
	ConsolidatedBytesPerNode metric.Int64Counter

	// UnprocessedBatches keeps track of the total number of batches pending consolidation
	UnprocessedBatches metric.Int64UpDownCounter

	// ConsolidationRunDuration tracks the time (in milliseconds) each consolidation run takes to process all batches
	ConsolidationRunDuration metric.Int64Histogram
)

// Init initializes the OpenTelemetry metrics with Prometheus exporter
func Init(environment string) error {
	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	// Create a resource with the environment attribute
	res := resource.NewSchemaless(attribute.String("env", environment))

	// Create a MeterProvider with the Prometheus exporter and resource
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)

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

	ConsolidationRunDuration, err = meter.Int64Histogram(
		"etracker_consolidation_run_duration_ms",
		metric.WithDescription("Time in milliseconds for each consolidation run to process all batches"),
	)
	if err != nil {
		return fmt.Errorf("failed to create ConsolidationRunDuration histogram: %w", err)
	}

	log.Info("OpenTelemetry metrics initialized with Prometheus exporter")
	return nil
}
