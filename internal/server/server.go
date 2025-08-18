package server

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"

	"github.com/storacha/payme/internal/service"
)

var log = logging.Logger("server")

type Server struct {
	ucantoSrv ucanto.ServerView
}

func New(id principal.Signer, svc *service.Service) (*Server, error) {
	ucantoSrv, err := ucanto.NewServer(id, serviceMethods(svc)...)
	if err != nil {
		return nil, err
	}

	return &Server{ucantoSrv: ucantoSrv}, nil
}

func (s *Server) ListenAndServe(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.getRootHandler())
	mux.HandleFunc("POST /payme", s.ucanHandler())

	log.Infof("Listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
