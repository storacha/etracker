package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/did"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/storacha/etracker/internal/db/consumer"
	"github.com/storacha/etracker/internal/db/customer"
	"github.com/storacha/etracker/internal/db/spacestats"
)

// Mock implementations for database tables

type mockCustomerTable struct {
	hasFunc func(ctx context.Context, customerDID did.DID) (bool, error)
}

func (m *mockCustomerTable) List(ctx context.Context, limit int, cursor *string) (*customer.ListResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockCustomerTable) Has(ctx context.Context, customerDID did.DID) (bool, error) {
	if m.hasFunc != nil {
		return m.hasFunc(ctx, customerDID)
	}
	return false, fmt.Errorf("not implemented")
}

var _ customer.CustomerTable = (*mockCustomerTable)(nil)

type mockConsumerTable struct {
	listByCustomerFunc func(ctx context.Context, customerID did.DID) ([]did.DID, error)
}

func (m *mockConsumerTable) Get(ctx context.Context, consumerID string) (consumer.Consumer, error) {
	return consumer.Consumer{}, fmt.Errorf("not implemented")
}

func (m *mockConsumerTable) ListByCustomer(ctx context.Context, customerID did.DID) ([]did.DID, error) {
	if m.listByCustomerFunc != nil {
		return m.listByCustomerFunc(ctx, customerID)
	}
	return nil, fmt.Errorf("not implemented")
}

var _ consumer.ConsumerTable = (*mockConsumerTable)(nil)

type mockSpaceStatsTable struct {
	getDailyStatsFunc func(ctx context.Context, space did.DID, from time.Time, to time.Time) ([]spacestats.DailyStats, error)
}

func (m *mockSpaceStatsTable) Record(ctx context.Context, space did.DID, egress uint64) error {
	return fmt.Errorf("not implemented")
}

func (m *mockSpaceStatsTable) GetDailyStats(ctx context.Context, space did.DID, from time.Time, to time.Time) ([]spacestats.DailyStats, error) {
	if m.getDailyStatsFunc != nil {
		return m.getDailyStatsFunc(ctx, space, from, to)
	}
	return nil, fmt.Errorf("not implemented")
}

var _ spacestats.SpaceStatsTable = (*mockSpaceStatsTable)(nil)

func TestGetAccountEgress(t *testing.T) {
	t.Run("returns account not found error when account doesn't exist", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return false, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
		}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		var accErr ErrAccountNotFound
		require.ErrorAs(t, err, &accErr)
		assert.Equal(t, accountDID, accErr.accountDID)
	})

	t.Run("returns error when customer table fails", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		expectedErr := fmt.Errorf("database error")

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return false, expectedErr
			},
		}

		svc := &service{
			customerTable: customerTable,
		}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, nil)

		require.Error(t, err)
		require.Nil(t, result)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("returns space unauthorized error when requested spaces don't belong to account", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		ownedSpace := testutil.RandomDID(t)
		unauthorizedSpace := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{ownedSpace}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, []did.DID{unauthorizedSpace}, nil)

		require.Error(t, err)
		require.Nil(t, result)
		var spaceErr ErrSpaceUnauthorized
		require.ErrorAs(t, err, &spaceErr)
		assert.Contains(t, spaceErr.unAuthSpaces, unauthorizedSpace)
	})

	t.Run("returns space unauthorized error for multiple unauthorized spaces", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		ownedSpace := testutil.RandomDID(t)
		unauth1 := testutil.RandomDID(t)
		unauth2 := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{ownedSpace}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, []did.DID{unauth1, unauth2}, nil)

		require.Error(t, err)
		require.Nil(t, result)
		var spaceErr ErrSpaceUnauthorized
		require.ErrorAs(t, err, &spaceErr)
		assert.Len(t, spaceErr.unAuthSpaces, 2)
		assert.Contains(t, spaceErr.unAuthSpaces, unauth1)
		assert.Contains(t, spaceErr.unAuthSpaces, unauth2)
	})

	t.Run("returns period not acceptable error when from is after to", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		from := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		period := &Period{From: from, To: to}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.Error(t, err)
		require.Nil(t, result)
		var periodErr ErrPeriodNotAcceptable
		require.ErrorAs(t, err, &periodErr)
		assert.Contains(t, periodErr.msg, "after")
	})

	t.Run("returns period not acceptable error when from equals to", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		period := &Period{From: date, To: date}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.Error(t, err)
		require.Nil(t, result)
		var periodErr ErrPeriodNotAcceptable
		require.ErrorAs(t, err, &periodErr)
	})

	t.Run("returns period not acceptable error when period exceeds 60 days", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC) // 61 days
		period := &Period{From: from, To: to}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.Error(t, err)
		require.Nil(t, result)
		var periodErr ErrPeriodNotAcceptable
		require.ErrorAs(t, err, &periodErr)
		assert.Contains(t, periodErr.msg, "60 days")
	})

	t.Run("successfully returns empty result for account with no spaces", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{}, nil
			},
		}

		svc := &service{
			customerTable: customerTable,
			consumerTable: consumerTable,
		}

		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, nil)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.Total)
		assert.Empty(t, result.Spaces)
	})

	t.Run("successfully aggregates stats from multiple spaces", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)

		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space1, space2}, nil
			},
		}

		spaceStatsTable := &mockSpaceStatsTable{
			getDailyStatsFunc: func(ctx context.Context, space did.DID, qFrom time.Time, qTo time.Time) ([]spacestats.DailyStats, error) {
				assert.Equal(t, from, qFrom)
				assert.Equal(t, to, qTo)

				if space == space1 {
					return []spacestats.DailyStats{
						{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 100},
						{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Egress: 200},
					}, nil
				}
				if space == space2 {
					return []spacestats.DailyStats{
						{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 300},
						{Date: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC), Egress: 400},
					}, nil
				}
				return nil, nil
			},
		}

		svc := &service{
			customerTable:   customerTable,
			consumerTable:   consumerTable,
			spaceStatsTable: spaceStatsTable,
		}

		period := &Period{From: from, To: to}
		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(1000), result.Total) // 100+200+300+400
		assert.Len(t, result.Spaces, 2)

		space1Data := result.Spaces[space1]
		assert.Equal(t, uint64(300), space1Data.Total)
		assert.Len(t, space1Data.DailyStats, 2)

		space2Data := result.Spaces[space2]
		assert.Equal(t, uint64(700), space2Data.Total)
		assert.Len(t, space2Data.DailyStats, 2)
	})

	t.Run("filters to requested spaces only", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)
		space3 := testutil.RandomDID(t)

		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space1, space2, space3}, nil
			},
		}

		spaceStatsTable := &mockSpaceStatsTable{
			getDailyStatsFunc: func(ctx context.Context, space did.DID, qFrom time.Time, qTo time.Time) ([]spacestats.DailyStats, error) {
				// Should only be called for space1
				require.Equal(t, space1, space)
				return []spacestats.DailyStats{
					{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 500},
				}, nil
			},
		}

		svc := &service{
			customerTable:   customerTable,
			consumerTable:   consumerTable,
			spaceStatsTable: spaceStatsTable,
		}

		period := &Period{From: from, To: to}
		result, err := svc.GetAccountEgress(context.Background(), accountDID, []did.DID{space1}, period)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(500), result.Total)
		assert.Len(t, result.Spaces, 1)
		assert.Contains(t, result.Spaces, space1)
	})

	t.Run("continues aggregation even if one space stats fetch fails", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)

		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space1, space2}, nil
			},
		}

		spaceStatsTable := &mockSpaceStatsTable{
			getDailyStatsFunc: func(ctx context.Context, space did.DID, qFrom time.Time, qTo time.Time) ([]spacestats.DailyStats, error) {
				if space == space1 {
					return nil, fmt.Errorf("database error")
				}
				return []spacestats.DailyStats{
					{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 300},
				}, nil
			},
		}

		svc := &service{
			customerTable:   customerTable,
			consumerTable:   consumerTable,
			spaceStatsTable: spaceStatsTable,
		}

		period := &Period{From: from, To: to}
		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.NoError(t, err)
		require.NotNil(t, result)
		// Only space2 stats are included
		assert.Equal(t, uint64(300), result.Total)
		assert.Len(t, result.Spaces, 1)
		assert.Contains(t, result.Spaces, space2)
	})

	t.Run("includes space with zero egress", func(t *testing.T) {
		accountDID := testutil.RandomDID(t)
		space := testutil.RandomDID(t)

		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

		customerTable := &mockCustomerTable{
			hasFunc: func(ctx context.Context, customerDID did.DID) (bool, error) {
				return true, nil
			},
		}

		consumerTable := &mockConsumerTable{
			listByCustomerFunc: func(ctx context.Context, customerID did.DID) ([]did.DID, error) {
				return []did.DID{space}, nil
			},
		}

		spaceStatsTable := &mockSpaceStatsTable{
			getDailyStatsFunc: func(ctx context.Context, s did.DID, qFrom time.Time, qTo time.Time) ([]spacestats.DailyStats, error) {
				// Return empty stats
				return []spacestats.DailyStats{}, nil
			},
		}

		svc := &service{
			customerTable:   customerTable,
			consumerTable:   consumerTable,
			spaceStatsTable: spaceStatsTable,
		}

		period := &Period{From: from, To: to}
		result, err := svc.GetAccountEgress(context.Background(), accountDID, nil, period)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.Total)
		assert.Len(t, result.Spaces, 1)
		assert.Contains(t, result.Spaces, space)
		assert.Equal(t, uint64(0), result.Spaces[space].Total)
		assert.Empty(t, result.Spaces[space].DailyStats)
	})
}
