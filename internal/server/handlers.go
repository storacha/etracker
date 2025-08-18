package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/storacha/go-ucanto/principal/signer"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"

	"github.com/storacha/payme/internal/build"
)

func (s *Server) getRootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("💸 payme %s\n", build.Version)))
		w.Write([]byte("- https://github.com/storacha/payme\n"))
		w.Write([]byte(fmt.Sprintf("- %s\n", s.ucantoSrv.ID().DID())))
		if ws, ok := s.ucantoSrv.ID().(signer.WrappedSigner); ok {
			w.Write([]byte(fmt.Sprintf("- %s\n", ws.Unwrap().DID())))
		}
	}
}

func (s *Server) ucanHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := s.ucantoSrv.Request(r.Context(), ucanhttp.NewHTTPRequest(r.Body, r.Header))
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
