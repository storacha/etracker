package web

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/http"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"github.com/storacha/etracker/internal/service"
)

var log = logging.Logger("web")

// Simple session store for authentication
type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]time.Time // sessionID -> expiry time
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		sessions: make(map[string]time.Time),
	}
}

func (s *sessionStore) createSession() string {
	b := make([]byte, 32)
	rand.Read(b)
	sessionID := base64.URLEncoding.EncodeToString(b)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = time.Now().Add(24 * time.Hour)
	return sessionID
}

func (s *sessionStore) validateSession(sessionID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	expiry, exists := s.sessions[sessionID]
	return exists && time.Now().Before(expiry)
}

func (s *sessionStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, expiry := range s.sessions {
		if now.After(expiry) {
			delete(s.sessions, id)
		}
	}
}

var globalSessionStore = newSessionStore()
var cleanupOnce sync.Once

// StatsService defines the interface for fetching provider and account statistics
type StatsService interface {
	GetAllProvidersStats(ctx context.Context, limit int, startToken *string) (*service.GetAllProvidersStatsResult, error)
	GetAllAccountsStats(ctx context.Context, limit int, startToken *string) (*service.GetAllAccountsStatsResult, error)
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
	ActiveTab           string
	Providers           []service.ProviderWithStats
	Accounts            []service.AccountStats
	NextToken           *string
	PrevToken           *string
	Error               string
	CSS                 template.CSS
	EgressDollarsPerTiB float64
}

type loginData struct {
	Error      string
	CSS        template.CSS
	RequestURL string
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
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDate(t interface{}) string {
	// Handle time.Time
	if v, ok := t.(interface{ Format(string) string }); ok {
		return v.Format("2006-01-02")
	}
	return fmt.Sprintf("%v", t)
}

func formatDollars(bytes uint64, dollarsPerTiB float64) string {
	const bytesPerTiB = 1024 * 1024 * 1024 * 1024
	dollars := (float64(bytes) / bytesPerTiB) * dollarsPerTiB
	return fmt.Sprintf("$%.2f", dollars)
}

// showLoginForm renders the login form
func showLoginForm(w http.ResponseWriter, r *http.Request, errorMsg string) {
	tmpl := template.Must(template.New("login").Parse(loginTemplateHTML))
	data := loginData{
		Error:      errorMsg,
		CSS:        template.CSS(loginCSS),
		RequestURL: r.URL.String(),
	}

	if err := tmpl.Execute(w, data); err != nil {
		log.Errorf("executing login template: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// BasicAuthMiddleware wraps an HTTP handler with basic authentication
func BasicAuthMiddleware(handler http.HandlerFunc, username, password string) http.HandlerFunc {
	// Start cleanup goroutine only once
	cleanupOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(1 * time.Hour)
			defer ticker.Stop()
			for range ticker.C {
				globalSessionStore.cleanup()
			}
		}()
	})

	return func(w http.ResponseWriter, r *http.Request) {
		// Enforce that credentials are configured
		if username == "" || password == "" {
			log.Error("Admin dashboard credentials not configured - denying access")
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
			return
		}

		// Check for existing session cookie
		if cookie, err := r.Cookie("session"); err == nil {
			if globalSessionStore.validateSession(cookie.Value) {
				handler(w, r)
				return
			}
		}

		// Handle login form submission (POST)
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				showLoginForm(w, r, "Invalid form data")
				return
			}

			formUser := r.FormValue("username")
			formPass := r.FormValue("password")

			// Use constant-time comparison to prevent timing attacks
			userMatch := subtle.ConstantTimeCompare([]byte(formUser), []byte(username)) == 1
			passMatch := subtle.ConstantTimeCompare([]byte(formPass), []byte(password)) == 1

			if !userMatch || !passMatch {
				showLoginForm(w, r, "Invalid username or password")
				return
			}

			// Authentication successful - create session and redirect
			sessionID := globalSessionStore.createSession()
			http.SetCookie(w, &http.Cookie{
				Name:     "session",
				Value:    sessionID,
				Path:     "/",
				HttpOnly: true,
				Secure:   r.TLS != nil,
				SameSite: http.SameSiteLaxMode,
				MaxAge:   86400, // 24 hours
			})

			// Redirect to the original URL (preserving query parameters)
			redirectURL := r.URL.String()
			if redirectURL == "" {
				redirectURL = "/admin"
			}
			http.Redirect(w, r, redirectURL, http.StatusSeeOther)
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
		showLoginForm(w, r, "")
	}
}

// AdminHandler returns an HTTP handler for the admin dashboard
func AdminHandler(svc StatsService, egressDollarsPerTiB float64) http.HandlerFunc {
	tmpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"formatBytes":   formatBytes,
		"formatDate":    formatDate,
		"formatDollars": formatDollars,
	}).Parse(adminTemplateHTML))

	const defaultLimit = 20

	return func(w http.ResponseWriter, r *http.Request) {
		data := adminDashboardData{
			CSS:                 template.CSS(adminCSS),
			EgressDollarsPerTiB: egressDollarsPerTiB,
		}

		// Get active tab from query parameter (default to "providers")
		tab := r.URL.Query().Get("tab")
		if tab == "" {
			tab = "providers"
		}
		data.ActiveTab = tab

		// Get pagination token from query parameter
		token := r.URL.Query().Get("token")
		var startToken *string
		if token != "" {
			startToken = &token
		}

		// Fetch data based on active tab
		switch tab {
		case "clients":
			result, err := svc.GetAllAccountsStats(r.Context(), defaultLimit, startToken)
			if err != nil {
				data.Error = fmt.Sprintf("Error fetching accounts: %v", err)
				if err := tmpl.Execute(w, data); err != nil {
					log.Errorf("executing admin template: %v", err)
					http.Error(w, "Internal server error", http.StatusInternalServerError)
				}
				return
			}

			data.Accounts = result.Accounts
			data.NextToken = result.NextToken
			if startToken != nil {
				data.PrevToken = startToken // For "back" navigation (simplified)
			}

		default: // "providers"
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
		}

		if err := tmpl.Execute(w, data); err != nil {
			log.Errorf("executing admin template: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}
