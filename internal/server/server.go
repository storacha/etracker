package server

import (
	"fmt"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/validator"

	"github.com/storacha/etracker/internal/consolidator"
	"github.com/storacha/etracker/internal/metrics"
	"github.com/storacha/etracker/internal/service"
	"github.com/storacha/etracker/web"
)

var log = logging.Logger("server")

type config struct {
	metricsEndpointToken    string
	adminUser               string
	adminPassword           string
	clientEgressUSDPerTiB   float64
	providerEgressUSDPerTiB float64
	principalResolver       validator.PrincipalResolver
	principalParser         validator.PrincipalParserFunc
	authProofs              []delegation.Delegation
}

type Option func(*config)

func WithMetricsEndpoint(authToken string) Option {
	return func(c *config) {
		c.metricsEndpointToken = authToken
	}
}

func WithAdminCreds(user, password string) Option {
	return func(c *config) {
		c.adminUser = user
		c.adminPassword = password
	}
}

func WithPricing(clientEgressUSDPerTiB, providerEgressUSDPerTiB float64) Option {
	return func(c *config) {
		c.clientEgressUSDPerTiB = clientEgressUSDPerTiB
		c.providerEgressUSDPerTiB = providerEgressUSDPerTiB
	}
}

func WithPrincipalResolver(resolver validator.PrincipalResolver) Option {
	return func(c *config) {
		c.principalResolver = resolver
	}
}

func WithPrincipalParser(parser validator.PrincipalParserFunc) Option {
	return func(c *config) {
		c.principalParser = parser
	}
}

func WithAuthorityProofs(authProofs ...delegation.Delegation) Option {
	return func(c *config) {
		c.authProofs = authProofs
	}
}

type Server struct {
	cfg       *config
	ucantoSrv ucanto.ServerView[ucanto.Service]
	cons      *consolidator.Consolidator
	svc       service.Service
}

func New(id principal.Signer, svc service.Service, cons *consolidator.Consolidator, opts ...Option) (*Server, error) {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	ucantoOpts := serviceMethods(svc)

	if cfg.principalResolver != nil {
		ucantoOpts = append(ucantoOpts, ucanto.WithPrincipalResolver(cfg.principalResolver.ResolveDIDKey))
	}

	if cfg.principalParser != nil {
		ucantoOpts = append(ucantoOpts, ucanto.WithPrincipalParser(cfg.principalParser))
	}

	ucantoOpts = append(ucantoOpts, ucanto.WithAuthorityProofs(cfg.authProofs...))

	ucantoSrv, err := ucanto.NewServer(id, ucantoOpts...)
	if err != nil {
		return nil, err
	}

	return &Server{cfg: cfg, ucantoSrv: ucantoSrv, cons: cons, svc: svc}, nil
}

func (s *Server) ListenAndServe(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.getRootHandler())
	mux.HandleFunc("POST /", s.ucanHandler())
	mux.HandleFunc("GET /receipts/{cid}", s.getReceiptsHandler())

	// Set up admin endpoint with authentication (handles both GET and POST)
	adminHandler := web.BasicAuthMiddleware(web.AdminHandler(s.svc, s.cfg.clientEgressUSDPerTiB, s.cfg.providerEgressUSDPerTiB), s.cfg.adminUser, s.cfg.adminPassword)
	mux.HandleFunc("GET /admin", adminHandler)
	mux.HandleFunc("POST /admin", adminHandler)

	if s.cfg.metricsEndpointToken != "" {
		if err := metrics.Init(); err != nil {
			return fmt.Errorf("initializing metrics: %w", err)
		}

		mux.Handle("GET /metrics", s.getMetricsHandler())
	} else {
		log.Warnf("Metrics endpoint is disabled")
	}

	// Wrap with CORS middleware
	corsHandler := corsMiddleware(mux)

	log.Infof("Listening on %s", addr)
	return http.ListenAndServe(addr, corsHandler)
}

// corsMiddleware adds CORS headers to allow cross-origin requests
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow all origins
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
