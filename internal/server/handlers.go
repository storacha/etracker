package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/signer"
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
		nodeDIDStr := r.URL.Query().Get("did")
		nodeDID, err := did.Parse(nodeDIDStr)
		if err != nil {
			http.Error(w, "invalid node DID", http.StatusBadRequest)
			return
		}

		cidStr := r.PathValue("cid")
		cid, err := cid.Decode(cidStr)
		if err != nil {
			http.Error(w, "invalid invocation CID", http.StatusBadRequest)
			return
		}

		cause := cidlink.Link{Cid: cid}

		rcpt, err := s.cons.GetReceipt(r.Context(), nodeDID, cause)
		if err != nil {
			if errors.Is(err, consolidator.ErrNotFound) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("getting receipt: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, err = io.Copy(w, rcpt.Archive())
		if err != nil {
			log.Errorf("sending receipt: %s", err)
		}
	}
}
