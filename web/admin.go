package web

import (
	_ "embed"
	"fmt"
	"html/template"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"

	"github.com/storacha/etracker/internal/service"
)

var log = logging.Logger("web")

//go:embed templates/admin.html.tmpl
var adminTemplateHTML string

//go:embed static/css/admin.css
var adminCSS string

type adminDashboardData struct {
	NodeDID string
	Stats   interface{}
	Error   string
	CSS     string
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
func AdminHandler(svc *service.Service) http.HandlerFunc {
	tmpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"formatBytes": formatBytes,
		"formatDate":  formatDate,
	}).Parse(adminTemplateHTML))

	return func(w http.ResponseWriter, r *http.Request) {
		data := adminDashboardData{
			CSS: adminCSS,
		}

		nodeDIDStr := r.URL.Query().Get("node")
		if nodeDIDStr == "" {
			// Show form only
			if err := tmpl.Execute(w, data); err != nil {
				log.Errorf("executing admin template: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		data.NodeDID = nodeDIDStr

		// Parse node DID
		nodeDID, err := did.Parse(nodeDIDStr)
		if err != nil {
			data.Error = fmt.Sprintf("Invalid node DID: %v", err)
			if err := tmpl.Execute(w, data); err != nil {
				log.Errorf("executing admin template: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		// Get stats from service
		stats, err := svc.GetStats(r.Context(), nodeDID)
		if err != nil {
			data.Error = fmt.Sprintf("Error fetching stats: %v", err)
			if err := tmpl.Execute(w, data); err != nil {
				log.Errorf("executing admin template: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		data.Stats = stats

		if err := tmpl.Execute(w, data); err != nil {
			log.Errorf("executing admin template: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}
