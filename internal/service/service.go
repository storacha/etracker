package service

import (
	"context"
	"net/url"
	"time"

	logging "github.com/ipfs/go-log/v2"
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
	"github.com/storacha/etracker/internal/db/spacestats"
	"github.com/storacha/etracker/internal/db/storageproviders"
	"github.com/storacha/etracker/internal/metrics"
)

var log = logging.Logger("service")

type Service struct {
	id                   principal.Signer
	environment          string
	egressTable          egress.EgressTable
	consolidatedTable    consolidated.ConsolidatedTable
	storageProviderTable storageproviders.StorageProviderTable
	customerTable        customer.CustomerTable
	consumerTable        consumer.ConsumerTable
	spaceStatsTable      spacestats.SpaceStatsTable
}

func New(
	id principal.Signer,
	environment string,
	egressTable egress.EgressTable,
	consolidatedTable consolidated.ConsolidatedTable,
	storageProviderTable storageproviders.StorageProviderTable,
	customerTable customer.CustomerTable,
	consumerTable consumer.ConsumerTable,
	spaceStatsTable spacestats.SpaceStatsTable,
) (*Service, error) {
	return &Service{
		id:                   id,
		environment:          environment,
		egressTable:          egressTable,
		consolidatedTable:    consolidatedTable,
		storageProviderTable: storageProviderTable,
		customerTable:        customerTable,
		consumerTable:        consumerTable,
		spaceStatsTable:      spaceStatsTable,
	}, nil
}

func (s *Service) Record(ctx context.Context, node did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	if err := s.egressTable.Record(ctx, receipts, node, endpoint, cause); err != nil {
		return err
	}

	attributes := attribute.NewSet(attribute.String("node", node.String()), attribute.String("env", s.environment))
	metrics.TrackedBatchesPerNode.Add(ctx, 1, metric.WithAttributeSet(attributes))

	return nil
}

func (s *Service) GetStats(ctx context.Context, node did.DID) (*Stats, error) {
	stats := NewStats(time.Now().UTC())

	// Get records from the beginning of previous month (earliest period we need)
	records, err := s.consolidatedTable.GetStatsByNode(ctx, node, stats.Earliest())
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		stats.AddEgress(record.TotalEgress, record.ProcessedAt)
	}

	return stats, nil
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

type AccountStats struct {
	Account    did.DID
	Stats      *Stats
	StatsError error // If there was an error fetching stats for this account
}

type GetAllAccountsStatsResult struct {
	Accounts  []AccountStats
	NextToken *string
}

func (s *Service) GetAllAccountsStats(ctx context.Context, limit int, startToken *string) (*GetAllAccountsStatsResult, error) {
	// Get customers with pagination
	result, err := s.customerTable.List(ctx, limit, startToken)
	if err != nil {
		return nil, err
	}

	// Fetch stats for each account
	accountsWithStats := make([]AccountStats, 0, len(result.Customers))
	for _, customerID := range result.Customers {
		stats, err := s.getAccountStats(ctx, customerID)

		accountsWithStats = append(accountsWithStats, AccountStats{
			Account:    customerID,
			Stats:      stats,
			StatsError: err, // Store error so we can show partial results
		})
	}

	return &GetAllAccountsStatsResult{
		Accounts:  accountsWithStats,
		NextToken: result.Cursor,
	}, nil
}

// getAccountStats calculates aggregated stats for an account by fetching all spaces and their daily stats
func (s *Service) getAccountStats(
	ctx context.Context,
	customerID did.DID,
) (*Stats, error) {
	stats := NewStats(time.Now().UTC())

	// Get all spaces (consumers) for this account
	spaces, err := s.consumerTable.ListByCustomer(ctx, customerID)
	if err != nil {
		return nil, err
	}

	// Aggregate stats across all spaces
	for _, space := range spaces {
		// Fetch daily stats for this space from the beginning of previous month
		dailyStats, err := s.spaceStatsTable.GetDailyStats(ctx, space, stats.Earliest())
		if err != nil {
			log.Error("failed to get daily stats for space", "space", space, "error", err)
			continue
		}

		for _, stat := range dailyStats {
			stats.AddEgress(stat.Egress, stat.Date)
		}
	}

	return stats, err
}
