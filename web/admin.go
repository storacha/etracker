package web

import (
	"context"
	_ "embed"
	"fmt"
	"html/template"
	"net/http"

	logging "github.com/ipfs/go-log/v2"

	"github.com/storacha/etracker/internal/service"
)

var log = logging.Logger("web")

// StatsService defines the interface for fetching provider statistics
type StatsService interface {
	GetAllProvidersStats(ctx context.Context, limit int, startToken *string) (*service.GetAllProvidersStatsResult, error)
}

//go:embed templates/admin.html.tmpl
var adminTemplateHTML string

//go:embed static/css/admin.css
var adminCSS string

type adminDashboardData struct {
	Providers []service.ProviderWithStats
	NextToken *string
	PrevToken *string
	Error     string
	CSS       template.CSS
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDate(t interface{}) string {
	// Handle time.Time
	if v, ok := t.(interface{ Format(string) string }); ok {
		return v.Format("2006-01-02 15:04 MST")
	}
	return fmt.Sprintf("%v", t)
}

// AdminHandler returns an HTTP handler for the admin dashboard
func AdminHandler(svc StatsService) http.HandlerFunc {
	tmpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"formatBytes": formatBytes,
		"formatDate":  formatDate,
	}).Parse(adminTemplateHTML))

	const defaultLimit = 20

	return func(w http.ResponseWriter, r *http.Request) {
		data := adminDashboardData{
			CSS: template.CSS(adminCSS),
		}

		// Get pagination token from query parameter
		token := r.URL.Query().Get("token")
		var startToken *string
		if token != "" {
			startToken = &token
		}

		// Fetch all providers with their stats
		result, err := svc.GetAllProvidersStats(r.Context(), defaultLimit, startToken)
		if err != nil {
			data.Error = fmt.Sprintf("Error fetching providers: %v", err)
			if err := tmpl.Execute(w, data); err != nil {
				log.Errorf("executing admin template: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		data.Providers = result.Providers
		data.NextToken = result.NextToken
		if startToken != nil {
			data.PrevToken = startToken // For "back" navigation (simplified)
		}

		if err := tmpl.Execute(w, data); err != nil {
			log.Errorf("executing admin template: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}
