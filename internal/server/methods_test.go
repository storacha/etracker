package server

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	accountegress "github.com/storacha/go-libstoracha/capabilities/account/egress"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/storacha/etracker/internal/service"
)

// mockService implements service.Service interface for testing
type mockService struct {
	getAccountEgressFunc     func(ctx context.Context, accountDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error)
	recordFunc               func(ctx context.Context, node did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error
	getStatsFunc             func(ctx context.Context, node did.DID) (*service.Stats, error)
	getAllProvidersStatsFunc func(ctx context.Context, limit int, startToken *string) (*service.GetAllProvidersStatsResult, error)
	getAllAccountsStatsFunc  func(ctx context.Context, limit int, startToken *string) (*service.GetAllAccountsStatsResult, error)
}

func (m *mockService) GetAccountEgress(ctx context.Context, accountDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
	if m.getAccountEgressFunc != nil {
		return m.getAccountEgressFunc(ctx, accountDID, spacesFilter, periodFilter)
	}
	return nil, fmt.Errorf("mockService.GetAccountEgress not implemented")
}

func (m *mockService) Record(ctx context.Context, node did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	if m.recordFunc != nil {
		return m.recordFunc(ctx, node, receipts, endpoint, cause)
	}
	return fmt.Errorf("mockService.Record not implemented")
}

func (m *mockService) GetStats(ctx context.Context, node did.DID) (*service.Stats, error) {
	if m.getStatsFunc != nil {
		return m.getStatsFunc(ctx, node)
	}
	return nil, fmt.Errorf("mockService.GetStats not implemented")
}

func (m *mockService) GetAllProvidersStats(ctx context.Context, limit int, startToken *string) (*service.GetAllProvidersStatsResult, error) {
	if m.getAllProvidersStatsFunc != nil {
		return m.getAllProvidersStatsFunc(ctx, limit, startToken)
	}
	return nil, fmt.Errorf("mockService.GetAllProvidersStats not implemented")
}

func (m *mockService) GetAllAccountsStats(ctx context.Context, limit int, startToken *string) (*service.GetAllAccountsStatsResult, error) {
	if m.getAllAccountsStatsFunc != nil {
		return m.getAllAccountsStatsFunc(ctx, limit, startToken)
	}
	return nil, fmt.Errorf("mockService.GetAllAccountsStats not implemented")
}

var _ service.Service = (*mockService)(nil)

func TestAccountEgressGetHandler(t *testing.T) {
	serviceSigner := testutil.WebService

	t.Run("successful invocation with no filters", func(t *testing.T) {
		// Setup
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)
		space2 := testutil.RandomDID(t)

		expectedData := &service.AccountEgress{
			Total: 1500,
			Spaces: map[did.DID]service.SpaceEgress{
				space1: {
					Total: 1000,
					DailyStats: []service.DailyStat{
						{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 500},
						{Date: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Egress: 500},
					},
				},
				space2: {
					Total: 500,
					DailyStats: []service.DailyStat{
						{Date: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Egress: 500},
					},
				},
			},
		}

		mockSvc := &mockService{
			getAccountEgressFunc: func(ctx context.Context, acctDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
				assert.Equal(t, accountDID, acctDID)
				assert.Empty(t, spacesFilter)
				assert.Nil(t, periodFilter)
				return expectedData, nil
			},
		}

		// Create server and connection
		conn, err := newTestConnection(serviceSigner, mockSvc)
		require.NoError(t, err)

		// Create invocation
		issuer := testutil.RandomSigner(t)
		inv, err := accountegress.Get.Invoke(
			issuer,
			serviceSigner,
			accountDID.String(),
			accountegress.GetCaveats{},
			delegation.WithNoExpiration(),
		)
		require.NoError(t, err)

		// Execute
		resp, err := client.Execute(context.Background(), []invocation.Invocation{inv}, conn)
		require.NoError(t, err)

		// Verify receipt
		rcptLink, ok := resp.Get(inv.Link())
		require.True(t, ok)

		reader, err := accountegress.NewGetReceiptReader()
		require.NoError(t, err)

		rcpt, err := reader.Read(rcptLink, resp.Blocks())
		require.NoError(t, err)
		require.NotNil(t, rcpt.Out())

		ok2, _ := result.Unwrap(rcpt.Out())

		assert.Equal(t, uint64(1500), ok2.Total)
		assert.Len(t, ok2.Spaces.Keys, 2)
		assert.Contains(t, ok2.Spaces.Keys, space1)
		assert.Contains(t, ok2.Spaces.Keys, space2)
		assert.Equal(t, uint64(1000), ok2.Spaces.Values[space1].Total)
		assert.Len(t, ok2.Spaces.Values[space1].DailyStats, 2)
		assert.Equal(t, uint64(500), ok2.Spaces.Values[space2].Total)
		assert.Len(t, ok2.Spaces.Values[space2].DailyStats, 1)
	})

	t.Run("successful invocation with spaces and period filters", func(t *testing.T) {
		// Setup
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)
		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

		expectedData := &service.AccountEgress{
			Total: 750,
			Spaces: map[did.DID]service.SpaceEgress{
				space1: {
					Total:      750,
					DailyStats: []service.DailyStat{},
				},
			},
		}

		mockSvc := &mockService{
			getAccountEgressFunc: func(ctx context.Context, acctDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
				assert.Equal(t, accountDID, acctDID)
				require.Len(t, spacesFilter, 1)
				assert.Contains(t, spacesFilter, space1)
				require.NotNil(t, periodFilter)
				assert.True(t, periodFilter.From.Equal(from))
				assert.True(t, periodFilter.To.Equal(to))
				return expectedData, nil
			},
		}

		// Create server and connection
		conn, err := newTestConnection(serviceSigner, mockSvc)
		require.NoError(t, err)

		// Create invocation
		issuer := testutil.RandomSigner(t)
		inv, err := accountegress.Get.Invoke(
			issuer,
			serviceSigner,
			accountDID.String(),
			accountegress.GetCaveats{
				Spaces: []did.DID{space1},
				Period: &accountegress.Period{
					From: from,
					To:   to,
				},
			},
			delegation.WithNoExpiration(),
		)
		require.NoError(t, err)

		// Execute
		resp, err := client.Execute(context.Background(), []invocation.Invocation{inv}, conn)
		require.NoError(t, err)

		// Verify receipt
		rcptLink, ok := resp.Get(inv.Link())
		require.True(t, ok)

		reader, err := accountegress.NewGetReceiptReader()
		require.NoError(t, err)

		rcpt, err := reader.Read(rcptLink, resp.Blocks())
		require.NoError(t, err)
		require.NotNil(t, rcpt.Out())

		ok2, _ := result.Unwrap(rcpt.Out())
		assert.Equal(t, uint64(750), ok2.Total)
	})

	t.Run("returns error when account not found", func(t *testing.T) {
		// Setup
		accountDID := testutil.RandomDID(t)

		mockSvc := &mockService{
			getAccountEgressFunc: func(ctx context.Context, acctDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
				return nil, service.NewAccountNotFoundError(accountDID)
			},
		}

		// Create server and connection
		conn, err := newTestConnection(serviceSigner, mockSvc)
		require.NoError(t, err)

		// Create invocation
		issuer := testutil.RandomSigner(t)
		inv, err := accountegress.Get.Invoke(
			issuer,
			serviceSigner,
			accountDID.String(),
			accountegress.GetCaveats{},
			delegation.WithNoExpiration(),
		)
		require.NoError(t, err)

		// Execute
		resp, err := client.Execute(context.Background(), []invocation.Invocation{inv}, conn)
		require.NoError(t, err)

		// Verify receipt
		rcptLink, ok := resp.Get(inv.Link())
		require.True(t, ok)

		reader, err := accountegress.NewGetReceiptReader()
		require.NoError(t, err)

		rcpt, err := reader.Read(rcptLink, resp.Blocks())
		require.NoError(t, err)
		require.NotNil(t, rcpt.Out())

		_, errVal := result.Unwrap(rcpt.Out())

		assert.Equal(t, accountegress.AccountNotFoundErrorName, errVal.ErrorName)
		assert.Contains(t, errVal.Message, accountDID.String())
	})

	t.Run("returns error when space unauthorized", func(t *testing.T) {
		// Setup
		accountDID := testutil.RandomDID(t)
		space1 := testutil.RandomDID(t)

		mockSvc := &mockService{
			getAccountEgressFunc: func(ctx context.Context, acctDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
				return nil, service.NewSpaceUnauthorizedError([]did.DID{space1})
			},
		}

		// Create server and connection
		conn, err := newTestConnection(serviceSigner, mockSvc)
		require.NoError(t, err)

		// Create invocation
		issuer := testutil.RandomSigner(t)
		inv, err := accountegress.Get.Invoke(
			issuer,
			serviceSigner,
			accountDID.String(),
			accountegress.GetCaveats{Spaces: []did.DID{space1}},
			delegation.WithNoExpiration(),
		)
		require.NoError(t, err)

		// Execute
		resp, err := client.Execute(context.Background(), []invocation.Invocation{inv}, conn)
		require.NoError(t, err)

		// Verify receipt
		rcptLink, ok := resp.Get(inv.Link())
		require.True(t, ok)

		reader, err := accountegress.NewGetReceiptReader()
		require.NoError(t, err)

		rcpt, err := reader.Read(rcptLink, resp.Blocks())
		require.NoError(t, err)
		require.NotNil(t, rcpt.Out())

		_, errVal := result.Unwrap(rcpt.Out())

		assert.Equal(t, accountegress.SpaceUnauthorizedErrorName, errVal.ErrorName)
		assert.Contains(t, errVal.Message, space1.String())
	})

	t.Run("returns error when period not acceptable", func(t *testing.T) {
		// Setup
		accountDID := testutil.RandomDID(t)
		from := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

		mockSvc := &mockService{
			getAccountEgressFunc: func(ctx context.Context, acctDID did.DID, spacesFilter []did.DID, periodFilter *service.Period) (*service.AccountEgress, error) {
				return nil, service.NewPeriodNotAcceptableError("'from' date is after 'to' date")
			},
		}

		// Create server and connection
		conn, err := newTestConnection(serviceSigner, mockSvc)
		require.NoError(t, err)

		// Create invocation
		issuer := testutil.RandomSigner(t)
		inv, err := accountegress.Get.Invoke(
			issuer,
			serviceSigner,
			accountDID.String(),
			accountegress.GetCaveats{
				Period: &accountegress.Period{From: from, To: to},
			},
			delegation.WithNoExpiration(),
		)
		require.NoError(t, err)

		// Execute
		resp, err := client.Execute(context.Background(), []invocation.Invocation{inv}, conn)
		require.NoError(t, err)

		// Verify receipt
		rcptLink, ok := resp.Get(inv.Link())
		require.True(t, ok)

		reader, err := accountegress.NewGetReceiptReader()
		require.NoError(t, err)

		rcpt, err := reader.Read(rcptLink, resp.Blocks())
		require.NoError(t, err)
		require.NotNil(t, rcpt.Out())

		_, errVal := result.Unwrap(rcpt.Out())

		assert.Equal(t, accountegress.PeriodNotAcceptableErrorName, errVal.ErrorName)
		assert.Contains(t, errVal.Message, "from")
	})
}

// newTestConnection creates a UCAN server and connection for testing
func newTestConnection(id principal.Signer, svc service.Service) (client.Connection, error) {
	opts := serviceMethods(svc)

	srv, err := ucanto.NewServer(id, opts...)
	if err != nil {
		return nil, err
	}
	return client.NewConnection(id, srv)
}
