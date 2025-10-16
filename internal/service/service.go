package service

import (
	"context"
	"net/url"
	"time"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/storacha/etracker/internal/db/consolidated"
	"github.com/storacha/etracker/internal/db/consumer"
	"github.com/storacha/etracker/internal/db/customer"
	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/db/storageproviders"
	"github.com/storacha/etracker/internal/metrics"
)

type Service struct {
	id                   principal.Signer
	egressTable          egress.EgressTable
	consolidatedTable    consolidated.ConsolidatedTable
	storageProviderTable storageproviders.StorageProviderTable
	customerTable        customer.CustomerTable
	consumerTable        consumer.ConsumerTable
}

func New(
	id principal.Signer,
	egressTable egress.EgressTable,
	consolidatedTable consolidated.ConsolidatedTable,
	storageProviderTable storageproviders.StorageProviderTable,
	customerTable customer.CustomerTable,
	consumerTable consumer.ConsumerTable,
) (*Service, error) {
	return &Service{
		id:                   id,
		egressTable:          egressTable,
		consolidatedTable:    consolidatedTable,
		storageProviderTable: storageProviderTable,
		customerTable:        customerTable,
		consumerTable:        consumerTable,
	}, nil
}

func (s *Service) Record(ctx context.Context, node did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	if err := s.egressTable.Record(ctx, receipts, node, endpoint, cause); err != nil {
		return err
	}

	attributes := attribute.NewSet(attribute.String("node", node.String()))
	metrics.TrackedBatchesPerNode.Add(ctx, 1, metric.WithAttributeSet(attributes))

	return nil
}

type Period struct {
	From time.Time
	To   time.Time
}

type PeriodStats struct {
	Egress uint64
	Period Period
}

type Stats struct {
	PreviousMonth PeriodStats
	CurrentMonth  PeriodStats
	CurrentWeek   PeriodStats
	CurrentDay    PeriodStats
}

func (s *Service) GetStats(ctx context.Context, node did.DID) (*Stats, error) {
	now := time.Now().UTC()

	// Calculate period boundaries
	currentYear, currentMonth, currentDay := now.Date()

	// Previous month: first day of previous month to last day of previous month
	previousMonthStart := time.Date(currentYear, currentMonth-1, 1, 0, 0, 0, 0, time.UTC)
	previousMonthEnd := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)

	// Current month: first day of current month to now
	currentMonthStart := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC)

	// Current week: start of week (Monday) to now
	weekday := now.Weekday()
	// Convert Sunday (0) to 7 for easier calculation
	if weekday == time.Sunday {
		weekday = 7
	}
	daysFromMonday := int(weekday) - 1
	currentWeekStart := time.Date(currentYear, currentMonth, currentDay-daysFromMonday, 0, 0, 0, 0, time.UTC)

	// Current day: start of day to now
	currentDayStart := time.Date(currentYear, currentMonth, currentDay, 0, 0, 0, 0, time.UTC)

	// Get records from the beginning of previous month (earliest period we need)
	records, err := s.consolidatedTable.GetStatsByNode(ctx, node, previousMonthStart)
	if err != nil {
		return nil, err
	}

	// Calculate stats for each period
	var previousMonthEgress, currentMonthEgress, currentWeekEgress, currentDayEgress uint64

	for _, record := range records {
		processedAt := record.ProcessedAt

		// Previous month
		if !processedAt.Before(previousMonthStart) && processedAt.Before(currentMonthStart) {
			previousMonthEgress += record.TotalEgress
		}

		// Current month
		if !processedAt.Before(currentMonthStart) {
			currentMonthEgress += record.TotalEgress
		}

		// Current week
		if !processedAt.Before(currentWeekStart) {
			currentWeekEgress += record.TotalEgress
		}

		// Current day
		if !processedAt.Before(currentDayStart) {
			currentDayEgress += record.TotalEgress
		}
	}

	return &Stats{
		PreviousMonth: PeriodStats{
			Egress: previousMonthEgress,
			Period: Period{
				From: previousMonthStart,
				To:   previousMonthEnd,
			},
		},
		CurrentMonth: PeriodStats{
			Egress: currentMonthEgress,
			Period: Period{
				From: currentMonthStart,
				To:   now,
			},
		},
		CurrentWeek: PeriodStats{
			Egress: currentWeekEgress,
			Period: Period{
				From: currentWeekStart,
				To:   now,
			},
		},
		CurrentDay: PeriodStats{
			Egress: currentDayEgress,
			Period: Period{
				From: currentDayStart,
				To:   now,
			},
		},
	}, nil
}

type ProviderWithStats struct {
	Provider   storageproviders.StorageProviderRecord
	Stats      *Stats
	StatsError error // If there was an error fetching stats for this provider
}

type GetAllProvidersStatsResult struct {
	Providers []ProviderWithStats
	NextToken *string
}

func (s *Service) GetAllProvidersStats(ctx context.Context, limit int, startToken *string) (*GetAllProvidersStatsResult, error) {
	// Get providers with pagination
	result, err := s.storageProviderTable.GetAll(ctx, limit, startToken)
	if err != nil {
		return nil, err
	}

	// Fetch stats for each provider
	providersWithStats := make([]ProviderWithStats, 0, len(result.Records))
	for _, provider := range result.Records {
		stats, err := s.GetStats(ctx, provider.Provider)

		providersWithStats = append(providersWithStats, ProviderWithStats{
			Provider:   provider,
			Stats:      stats,
			StatsError: err, // Store error so we can show partial results
		})
	}

	return &GetAllProvidersStatsResult{
		Providers: providersWithStats,
		NextToken: result.NextToken,
	}, nil
}
