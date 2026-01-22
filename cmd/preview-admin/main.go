package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/etracker/internal/db/storageproviders"
	"github.com/storacha/etracker/internal/service"
	"github.com/storacha/etracker/web"
)

// mockService implements a mock version of the service for preview purposes
type mockService struct{}

func (m *mockService) getMockStats(node did.DID, multiplier float64) *service.Stats {
	now := time.Now().UTC()
	currentYear, currentMonth, currentDay := now.Date()

	// Mock data - showing large egress values (TiB to PiB range) with variation per provider
	// Base is ~50 TiB, multipliers will create range from ~15 TiB to ~1.25 PiB
	baseEgress := uint64(float64(54975581388800) * multiplier)

	return &service.Stats{
		PreviousMonth: service.PeriodStats{
			Egress: baseEgress,
			Period: service.Period{
				From: time.Date(currentYear, currentMonth-1, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second),
			},
		},
		CurrentMonth: service.PeriodStats{
			Egress: baseEgress / 2,
			Period: service.Period{
				From: time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC),
				To:   now,
			},
		},
		CurrentWeek: service.PeriodStats{
			Egress: baseEgress / 7,
			Period: service.Period{
				From: now.AddDate(0, 0, -int(now.Weekday())),
				To:   now,
			},
		},
		CurrentDay: service.PeriodStats{
			Egress: baseEgress / 30,
			Period: service.Period{
				From: time.Date(currentYear, currentMonth, currentDay, 0, 0, 0, 0, time.UTC),
				To:   now,
			},
		},
	}
}

func (m *mockService) GetAllProvidersStats(ctx context.Context, limit int, startToken *string) (*service.GetAllProvidersStatsResult, error) {
	// Create mock providers
	mockProviders := []storageproviders.StorageProviderRecord{
		{
			Provider:      must(did.Parse("did:key:z6MkrZ1r5XBFZjBU34qyD8fueMbMRkKw17BZaq2ivKFjnz2z")),
			OperatorEmail: "operator1@example.com",
			Endpoint:      "https://node1.storage.example.com",
			WalletAddress: "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb1",
		},
		{
			Provider:      must(did.Parse("did:key:z6MkwCQm4mGfvAQJ9FzQb5nR5qZ7VHmGQG3dFfvGH5xnU3Rr")),
			OperatorEmail: "operator2@example.com",
			Endpoint:      "https://node2.storage.example.com",
			WalletAddress: "0x8Ba1f109551bD432803012645Ac136ddd64DBA72",
		},
		{
			Provider:      must(did.Parse("did:key:z6MkfQ7kBJpPFZzLvXHGmF2nqC9v8eUxPRjzUgVZYQxQz3Kk")),
			OperatorEmail: "operator3@example.com",
			Endpoint:      "https://node3.storage.example.com",
			WalletAddress: "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
		},
		{
			Provider:      must(did.Parse("did:key:z6MkpTRfBGbZGJtQ2VXmV5qZ7VHmFxZ9LkH4JcNz8QdKr2Mm")),
			OperatorEmail: "operator4@example.com",
			Endpoint:      "https://node4.storage.example.com",
			WalletAddress: "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
		},
		{
			Provider:      must(did.Parse("did:key:z6MknHN3fvBZzG9QrZ5nJ8LxHmFxZ9LkH4JcNz8QdKr2Yy7p")),
			OperatorEmail: "operator5@example.com",
			Endpoint:      "https://node5.storage.example.com",
			WalletAddress: "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
		},
	}

	// Build providers with stats - with varied data
	providersWithStats := make([]service.ProviderWithStats, 0, len(mockProviders))
	multipliers := []float64{1.5, 5.0, 12.0, 0.3, 25.0} // Different traffic levels from ~15 TiB to ~1.25 PiB
	for i, provider := range mockProviders {
		stats := m.getMockStats(provider.Provider, multipliers[i])
		providersWithStats = append(providersWithStats, service.ProviderWithStats{
			Provider:   provider,
			Stats:      stats,
			StatsError: nil,
		})
	}

	// Simple pagination: no next token for mock data
	return &service.GetAllProvidersStatsResult{
		Providers: providersWithStats,
		NextToken: nil,
	}, nil
}

func (m *mockService) GetAllAccountsStats(ctx context.Context, limit int, startToken *string) (*service.GetAllAccountsStatsResult, error) {
	// Create mock accounts (customers) - using mailto DIDs
	mockAccounts := []did.DID{
		must(did.Parse("did:mailto:example.com:alice")),
		must(did.Parse("did:mailto:example.com:bob")),
		must(did.Parse("did:mailto:acme.org:charlie")),
		must(did.Parse("did:mailto:startup.io:david")),
		must(did.Parse("did:mailto:enterprise.com:eve")),
		must(did.Parse("did:mailto:company.net:frank")),
		must(did.Parse("did:mailto:business.io:grace")),
	}

	// Build accounts with stats - with varied data
	accountsWithStats := make([]service.AccountStats, 0, len(mockAccounts))
	multipliers := []float64{8.0, 3.5, 0.8, 15.0, 1.2, 20.0, 0.4} // Different usage levels from ~20 TiB to ~1 PiB
	for i, account := range mockAccounts {
		stats := m.getMockStats(account, multipliers[i])
		accountsWithStats = append(accountsWithStats, service.AccountStats{
			Account:    account,
			Stats:      stats,
			StatsError: nil,
		})
	}

	// Simple pagination: no next token for mock data
	return &service.GetAllAccountsStatsResult{
		Accounts:  accountsWithStats,
		NextToken: nil,
	}, nil
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func main() {
	port := "8080"

	// Create mock service
	mockSvc := &mockService{}

	// Default credentials for preview
	username := "admin"
	password := "password"

	// Create HTTP server
	mux := http.NewServeMux()

	// Add debugging endpoint to check embedded assets
	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Debug Info:\n")
		fmt.Fprintf(w, "CSS embedded: checking web package...\n")
		// Note: We can't directly access the embedded CSS from this package
		// but the handler should work if embed is correct
	})

	// Wrap admin handler with authentication
	// Use a default pricing value for preview ($10 per TiB)
	adminHandler := web.BasicAuthMiddleware(web.AdminHandler(mockSvc, 10.0), username, password)
	mux.HandleFunc("/admin", adminHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin", http.StatusFound)
	})

	fmt.Printf("🎨 Admin Dashboard Preview Server\n")
	fmt.Printf("   Visit: http://localhost:%s/admin\n", port)
	fmt.Printf("   Login with:\n")
	fmt.Printf("     Username: %s\n", username)
	fmt.Printf("     Password: %s\n", password)
	fmt.Printf("   Shows all storage providers with their billing stats\n\n")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
