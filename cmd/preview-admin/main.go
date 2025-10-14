package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/etracker/internal/service"
	"github.com/storacha/etracker/web"
)

// mockService implements a mock version of the service for preview purposes
type mockService struct{}

func (m *mockService) GetStats(ctx context.Context, node did.DID) (*service.Stats, error) {
	now := time.Now().UTC()
	currentYear, currentMonth, currentDay := now.Date()

	// Mock data - showing realistic egress values
	return &service.Stats{
		PreviousMonth: service.PeriodStats{
			Egress: 156784325632, // ~146 GB
			Period: service.Period{
				From: time.Date(currentYear, currentMonth-1, 1, 0, 0, 0, 0, time.UTC),
				To:   time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second),
			},
		},
		CurrentMonth: service.PeriodStats{
			Egress: 89432158720, // ~83 GB
			Period: service.Period{
				From: time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC),
				To:   now,
			},
		},
		CurrentWeek: service.PeriodStats{
			Egress: 23654789120, // ~22 GB
			Period: service.Period{
				From: now.AddDate(0, 0, -int(now.Weekday())),
				To:   now,
			},
		},
		CurrentDay: service.PeriodStats{
			Egress: 3845632000, // ~3.6 GB
			Period: service.Period{
				From: time.Date(currentYear, currentMonth, currentDay, 0, 0, 0, 0, time.UTC),
				To:   now,
			},
		},
	}, nil
}

func main() {
	port := "8080"

	// Create mock service
	mockSvc := &mockService{}

	// Create HTTP server
	mux := http.NewServeMux()

	// Add debugging endpoint to check embedded assets
	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Debug Info:\n")
		fmt.Fprintf(w, "CSS embedded: checking web package...\n")
		// Note: We can't directly access the embedded CSS from this package
		// but the handler should work if embed is correct
	})

	mux.HandleFunc("/admin", web.AdminHandler(mockSvc))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin", http.StatusFound)
	})

	fmt.Printf("🎨 Admin Dashboard Preview Server\n")
	fmt.Printf("   Visit: http://localhost:%s/admin\n", port)
	fmt.Printf("   Try with: ?node=did:key:z6MkexampleNodeDIDForPreview\n\n")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
