package web

import (
	"context"
	"crypto/subtle"
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

//go:embed templates/login.html.tmpl
var loginTemplateHTML string

//go:embed static/css/login.css
var loginCSS string

type adminDashboardData struct {
	Providers []service.ProviderWithStats
	NextToken *string
	PrevToken *string
	Error     string
	CSS       template.CSS
}

type loginData struct {
	Error string
	CSS   template.CSS
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

// showLoginForm renders the login form
func showLoginForm(w http.ResponseWriter, errorMsg string) {
	tmpl := template.Must(template.New("login").Parse(loginTemplateHTML))
	data := loginData{
		Error: errorMsg,
		CSS:   template.CSS(loginCSS),
	}

	if err := tmpl.Execute(w, data); err != nil {
		log.Errorf("executing login template: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// BasicAuthMiddleware wraps an HTTP handler with basic authentication
func BasicAuthMiddleware(handler http.HandlerFunc, username, password string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Enforce that credentials are configured
		if username == "" || password == "" {
			log.Error("Admin dashboard credentials not configured - denying access")
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		}

		// Handle login form submission (POST)
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				showLoginForm(w, "Invalid form data")
				return
			}

			formUser := r.FormValue("username")
			formPass := r.FormValue("password")

			// Use constant-time comparison to prevent timing attacks
			userMatch := subtle.ConstantTimeCompare([]byte(formUser), []byte(username)) == 1
			passMatch := subtle.ConstantTimeCompare([]byte(formPass), []byte(password)) == 1

			if !userMatch || !passMatch {
				showLoginForm(w, "Invalid username or password")
				return
			}

			// Authentication successful, proceed to handler
			handler(w, r)
			return
		}

		// GET request - check for basic auth header
		user, pass, ok := r.BasicAuth()
		if ok {
			// Use constant-time comparison to prevent timing attacks
			userMatch := subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1
			passMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(password)) == 1

			if userMatch && passMatch {
				handler(w, r)
				return
			}
		}

		// No valid credentials - show login form
		showLoginForm(w, "")
	}
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
