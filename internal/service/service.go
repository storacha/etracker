package service

import (
	"context"
	"net/url"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/metrics"
)

type Service struct {
	id          principal.Signer
	egressTable egress.EgressTable
}

func New(id principal.Signer, egressTable egress.EgressTable) (*Service, error) {
	return &Service{id: id, egressTable: egressTable}, nil
}

func (s *Service) Record(ctx context.Context, nodeDID did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	if err := s.egressTable.Record(ctx, nodeDID, receipts, endpoint, cause); err != nil {
		return err
	}

	attributes := attribute.NewSet(attribute.String("node_id", nodeDID.String()))
	metrics.TrackedBatchesPerNode.Add(ctx, 1, metric.WithAttributeSet(attributes))

	return nil
}
