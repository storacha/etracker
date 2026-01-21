package service

import (
	"context"
	"fmt"
	"net/url"
	"slices"
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

type ErrAccountNotFound struct {
	msg string
}

func NewAccountNotFoundError(msg string) ErrAccountNotFound {
	return ErrAccountNotFound{msg: msg}
}

func (e ErrAccountNotFound) Error() string {
	return e.msg
}

type ErrPeriodNotAcceptable struct {
	msg string
}

func NewPeriodNotAcceptableError(msg string) ErrPeriodNotAcceptable {
	return ErrPeriodNotAcceptable{msg: msg}
}

func (e ErrPeriodNotAcceptable) Error() string {
	return e.msg
}

type ErrSpaceUnauthorized struct {
	msg string
}

func NewSpaceUnauthorizedError(msg string) ErrSpaceUnauthorized {
	return ErrSpaceUnauthorized{msg: msg}
}

func (e ErrSpaceUnauthorized) Error() string {
	return e.msg
}

// SpaceEgress holds egress data for a single space
type SpaceEgress struct {
	Total      uint64
	DailyStats []DailyStat
}

// DailyStat represents egress for a single day
type DailyStat struct {
	Date   time.Time
	Egress uint64
}

// AccountEgress holds complete egress data for an account
type AccountEgress struct {
	Total  uint64
	Spaces map[did.DID]SpaceEgress
}

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
		// Fetch daily stats for this space from the beginning of previous month to now
		dailyStats, err := s.spaceStatsTable.GetDailyStats(ctx, space, stats.Earliest(), time.Now().UTC())
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

const maxPeriodDays = 60

// defaultPeriod returns a period from the first day of the last complete month to today
func defaultPeriod() Period {
	now := time.Now().UTC()

	// Calculate first day of last complete month
	firstOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	firstOfLastMonth := firstOfThisMonth.AddDate(0, -1, 0)

	return Period{
		From: firstOfLastMonth,
		To:   now,
	}
}

// GetAccountEgress fetches egress data for an account with optional filters
func (s *Service) GetAccountEgress(
	ctx context.Context,
	accountDID did.DID,
	spacesFilter []did.DID,
	periodFilter *Period,
) (*AccountEgress, error) {
	// 1. Validate account exists
	exists, err := s.customerTable.Has(ctx, accountDID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, NewAccountNotFoundError(fmt.Sprintf("customer account %s not found", accountDID))
	}

	// 2. Determine which spaces to query
	allSpaces, err := s.consumerTable.ListByCustomer(ctx, accountDID)
	if err != nil {
		return nil, err
	}
	spacesToQuery := allSpaces
	if len(spacesFilter) != 0 {
		// Validate requested spaces belong to account
		for _, s := range spacesFilter {
			if !slices.Contains(allSpaces, s) {
				return nil, NewSpaceUnauthorizedError(fmt.Sprintf("space %s is not owned by account %s", s, accountDID))
			}
		}

		spacesToQuery = spacesFilter
	}

	// 3. If no spaces, return success with zeros (as per requirement)
	if len(spacesToQuery) == 0 {
		return &AccountEgress{
			Total:  0,
			Spaces: make(map[did.DID]SpaceEgress),
		}, nil
	}

	// 4. Determine query time range
	// Default: first day of last complete month to today
	period := defaultPeriod()
	if periodFilter != nil {
		from := time.Date(periodFilter.From.Year(), periodFilter.From.Month(), periodFilter.From.Day(), 0, 0, 0, 0, period.From.Location())
		to := time.Date(periodFilter.To.Year(), periodFilter.To.Month(), periodFilter.To.Day(), 0, 0, 0, 0, period.To.Location())
		if from.After(to) || from.Equal(to) {
			return nil, NewPeriodNotAcceptableError(fmt.Sprintf("'from' date %s is after or same as 'to' date %s", from, to))
		}

		daysBetween := int(to.Sub(from).Hours() / 24)
		if daysBetween > maxPeriodDays {
			return nil, NewPeriodNotAcceptableError(fmt.Sprintf("requested period exceeds maximum of %d days", maxPeriodDays))
		}

		period = *periodFilter
	}

	// 5. Fetch and aggregate stats for each space
	var totalEgress uint64
	spacesData := make(map[did.DID]SpaceEgress, len(spacesToQuery))

	for _, spaceDID := range spacesToQuery {
		dailyStatsDB, err := s.spaceStatsTable.GetDailyStats(ctx, spaceDID, period.From, period.To)
		if err != nil {
			log.Errorf("failed to get daily stats for space %s: %v", spaceDID, err)
			continue // Skip space but continue with others
		}

		// No need to filter - DB query already filtered by BETWEEN
		var spaceTotal uint64
		dailyStats := make([]DailyStat, 0, len(dailyStatsDB))

		for _, dbStat := range dailyStatsDB {
			spaceTotal += dbStat.Egress
			dailyStats = append(dailyStats, DailyStat{
				Date:   dbStat.Date,
				Egress: dbStat.Egress,
			})
		}

		// Include space even if it has no data - shows space exists but has zero egress
		spacesData[spaceDID] = SpaceEgress{
			Total:      spaceTotal,
			DailyStats: dailyStats,
		}
		totalEgress += spaceTotal
	}

	return &AccountEgress{
		Total:  totalEgress,
		Spaces: spacesData,
	}, nil
}
