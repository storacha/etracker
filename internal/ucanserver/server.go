package ucanserver

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"
)

var log = logging.Logger("ucanserver")

type UCANServer struct {
	id        principal.Signer
	ucantoSrv ucanto.ServerView
}

func New(id principal.Signer) (*UCANServer, error) {
	ucantoSrv, err := ucanto.NewServer(id, serviceMethods...)
	if err != nil {
		return nil, err
	}

	return &UCANServer{id: id, ucantoSrv: ucantoSrv}, nil
}

func (us *UCANServer) ListenAndServe(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", us.getRootHandler())
	mux.HandleFunc("POST /payme", us.postPayMeHandler())

	log.Infof("Listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
