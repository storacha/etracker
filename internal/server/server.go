package server

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"

	"github.com/storacha/etracker/internal/presets"
	"github.com/storacha/etracker/internal/service"
)

var log = logging.Logger("server")

type Server struct {
	ucantoSrv ucanto.ServerView[ucanto.Service]
}

func New(id principal.Signer, svc *service.Service) (*Server, error) {
	opts := serviceMethods(svc)

	presolver, err := presets.NewPresetResolver()
	if err != nil {
		return nil, err
	}
	opts = append(opts, ucanto.WithPrincipalResolver(presolver.ResolveDIDKey))

	ucantoSrv, err := ucanto.NewServer(id, opts...)
	if err != nil {
		return nil, err
	}

	return &Server{ucantoSrv: ucantoSrv}, nil
}

func (s *Server) ListenAndServe(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.getRootHandler())
	mux.HandleFunc("POST /track", s.ucanHandler())
	mux.HandleFunc("GET /receipts/{cid}", s.getReceiptsHandler())

	log.Infof("Listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
