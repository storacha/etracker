package server

import (
	"context"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	userver "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/capabilities/space/egress"
	"github.com/storacha/etracker/internal/service"
)

func serviceMethods(svc *service.Service) []userver.Option {
	return []userver.Option{
		userver.WithServiceMethod(
			egress.TrackAbility,
			userver.Provide(egress.Track, ucanTrackHandler(svc)),
		),
	}
}

func ucanTrackHandler(svc *service.Service) func(ctx context.Context, cap ucan.Capability[egress.TrackCaveats], inv invocation.Invocation, ictx userver.InvocationContext) (egress.TrackOk, fx.Effects, error) {
	return func(ctx context.Context, cap ucan.Capability[egress.TrackCaveats], inv invocation.Invocation, ictx userver.InvocationContext) (egress.TrackOk, fx.Effects, error) {
		nodeDID := inv.Issuer().DID()
		receipts := cap.Nb().Receipts
		endpoint := cap.Nb().Endpoint

		err := svc.Record(ctx, nodeDID, receipts, endpoint)
		if err != nil {
			return egress.TrackOk{}, nil, err
		}

		return egress.TrackOk{}, nil, nil
	}
}
