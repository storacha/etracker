package service

import (
	"context"
	"net/url"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/db/egress"
)

type Service struct {
	id          principal.Signer
	egressTable egress.EgressTable
}

func New(id principal.Signer, egressTable egress.EgressTable) (*Service, error) {
	return &Service{id: id, egressTable: egressTable}, nil
}

func (s *Service) Record(ctx context.Context, nodeDID did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	return s.egressTable.Record(ctx, nodeDID, receipts, endpoint, cause)
}
