package server

import (
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/signer"
	"github.com/storacha/go-ucanto/transport/car/response"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"

	"github.com/storacha/etracker/internal/build"
	"github.com/storacha/etracker/internal/consolidator"
)

func (s *Server) getRootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "💸 etracker %s\n", build.Version)
		fmt.Fprint(w, "- https://github.com/storacha/etracker\n")
		fmt.Fprintf(w, "- %s\n", s.ucantoSrv.ID().DID())
		if ws, ok := s.ucantoSrv.ID().(signer.WrappedSigner); ok {
			fmt.Fprintf(w, "- %s\n", ws.Unwrap().DID())
		}
	}
}

func (s *Server) ucanHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := s.ucantoSrv.Request(r.Context(), ucanhttp.NewRequest(r.Body, r.Header))
		if err != nil {
			log.Errorf("handling UCAN request: %s", err)
		}

		for key, vals := range res.Headers() {
			for _, v := range vals {
				w.Header().Add(key, v)
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		_, err = io.Copy(w, res.Body())
		if err != nil {
			log.Errorf("sending UCAN response: %s", err)
		}
	}
}

func (s *Server) getReceiptsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cidStr := r.PathValue("cid")
		cid, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(w, "invalid invocation CID", http.StatusBadRequest)
			return
		}

		cause := cidlink.Link{Cid: cid}

		rcpt, err := s.cons.GetReceipt(r.Context(), cause)
		if err != nil {
			if errors.Is(err, consolidator.ErrNotFound) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("getting receipt: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
		if err != nil {
			log.Errorf("building message: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		res, err := response.Encode(msg)
		if err != nil {
			log.Errorf("encoding receipt message: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, err = io.Copy(w, res.Body())
		if err != nil {
			log.Errorf("sending receipt: %v", err)
		}
	}
}

func (s *Server) getMetricsHandler() http.HandlerFunc {
	promHandler := promhttp.Handler()

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer %s", s.cfg.metricsEndpointToken) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		promHandler.ServeHTTP(w, r)
	}
}

const adminDashboardTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>etracker Admin Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            text-align: center;
            color: white;
            margin-bottom: 40px;
        }
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        .header p {
            font-size: 1.1em;
            opacity: 0.9;
        }
        .card {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
            margin-bottom: 30px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            font-weight: 600;
            margin-bottom: 8px;
            color: #333;
        }
        input[type="text"] {
            width: 100%;
            padding: 12px 16px;
            border: 2px solid #e1e4e8;
            border-radius: 8px;
            font-size: 1em;
            transition: border-color 0.3s;
        }
        input[type="text"]:focus {
            outline: none;
            border-color: #667eea;
        }
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 32px;
            border-radius: 8px;
            font-size: 1em;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s;
        }
        button:hover {
            transform: translateY(-2px);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 24px;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        }
        .stat-card h3 {
            font-size: 0.9em;
            opacity: 0.9;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .stat-card .value {
            font-size: 2em;
            font-weight: 700;
            margin-bottom: 8px;
        }
        .stat-card .period {
            font-size: 0.85em;
            opacity: 0.8;
        }
        .error {
            background: #fee;
            color: #c33;
            padding: 16px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid #c33;
        }
        .node-info {
            background: #f6f8fa;
            padding: 16px;
            border-radius: 8px;
            margin-bottom: 24px;
            word-break: break-all;
        }
        .node-info strong {
            color: #667eea;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>💸 etracker</h1>
            <p>Admin Dashboard - Node Statistics & Billing Metrics</p>
        </div>

        <div class="card">
            <form method="GET" action="/admin">
                <div class="form-group">
                    <label for="node">Node DID</label>
                    <input type="text" id="node" name="node" placeholder="did:key:..." value="{{.NodeDID}}" required>
                </div>
                <button type="submit">Get Stats</button>
            </form>
        </div>

        {{if .Error}}
        <div class="card">
            <div class="error">{{.Error}}</div>
        </div>
        {{end}}

        {{if .Stats}}
        <div class="card">
            <div class="node-info">
                <strong>Node:</strong> {{.NodeDID}}
            </div>

            <h2 style="margin-bottom: 20px;">Node Statistics</h2>

            <div class="stats-grid">
                <div class="stat-card">
                    <h3>Current Day</h3>
                    <div class="value">{{.Stats.CurrentDay.Egress | formatBytes}}</div>
                    <div class="period">{{.Stats.CurrentDay.Period.From | formatDate}} - {{.Stats.CurrentDay.Period.To | formatDate}}</div>
                </div>

                <div class="stat-card">
                    <h3>Current Week</h3>
                    <div class="value">{{.Stats.CurrentWeek.Egress | formatBytes}}</div>
                    <div class="period">{{.Stats.CurrentWeek.Period.From | formatDate}} - {{.Stats.CurrentWeek.Period.To | formatDate}}</div>
                </div>

                <div class="stat-card">
                    <h3>Current Month</h3>
                    <div class="value">{{.Stats.CurrentMonth.Egress | formatBytes}}</div>
                    <div class="period">{{.Stats.CurrentMonth.Period.From | formatDate}} - {{.Stats.CurrentMonth.Period.To | formatDate}}</div>
                </div>

                <div class="stat-card">
                    <h3>Previous Month</h3>
                    <div class="value">{{.Stats.PreviousMonth.Egress | formatBytes}}</div>
                    <div class="period">{{.Stats.PreviousMonth.Period.From | formatDate}} - {{.Stats.PreviousMonth.Period.To | formatDate}}</div>
                </div>
            </div>
        </div>
        {{end}}
    </div>
</body>
</html>`

type adminDashboardData struct {
	NodeDID string
	Stats   interface{}
	Error   string
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

func (s *Server) getAdminHandler() http.HandlerFunc {
	tmpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"formatBytes": formatBytes,
		"formatDate":  formatDate,
	}).Parse(adminDashboardTemplate))

	return func(w http.ResponseWriter, r *http.Request) {
		data := adminDashboardData{}

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
		stats, err := s.svc.GetStats(r.Context(), nodeDID)
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
