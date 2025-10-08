package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/storacha/go-ucanto/principal/signer"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"

	"github.com/storacha/etracker/internal/build"
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
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_ = cidlink.Link{Cid: cid}

		// TODO: fetch receipt from DB
		w.WriteHeader(http.StatusNotFound)
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
