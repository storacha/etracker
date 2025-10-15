package server

import (
	"fmt"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"

	"github.com/storacha/etracker/internal/consolidator"
	"github.com/storacha/etracker/internal/metrics"
	"github.com/storacha/etracker/internal/presets"
	"github.com/storacha/etracker/internal/service"
	"github.com/storacha/etracker/web"
)

var log = logging.Logger("server")

type config struct {
	metricsEndpointToken string
	adminUser            string
	adminPassword        string
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

type Server struct {
	cfg       *config
	ucantoSrv ucanto.ServerView[ucanto.Service]
	cons      *consolidator.Consolidator
	svc       *service.Service
}

func New(id principal.Signer, svc *service.Service, cons *consolidator.Consolidator, opts ...Option) (*Server, error) {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	ucantoOpts := serviceMethods(svc)

	presolver, err := presets.NewPresetResolver()
	if err != nil {
		return nil, err
	}
	ucantoOpts = append(ucantoOpts, ucanto.WithPrincipalResolver(presolver.ResolveDIDKey))

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
	adminHandler := web.BasicAuthMiddleware(web.AdminHandler(s.svc), s.cfg.adminUser, s.cfg.adminPassword)
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

	log.Infof("Listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
