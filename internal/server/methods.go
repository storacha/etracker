package server

import (
	"context"

	"github.com/storacha/go-libstoracha/capabilities/space/egress"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	userver "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

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

func ucanTrackHandler(svc *service.Service) func(
	ctx context.Context,
	cap ucan.Capability[egress.TrackCaveats],
	inv invocation.Invocation,
	ictx userver.InvocationContext,
) (result.Result[egress.TrackOk, egress.TrackError], fx.Effects, error) {
	return func(
		ctx context.Context,
		cap ucan.Capability[egress.TrackCaveats],
		inv invocation.Invocation,
		ictx userver.InvocationContext,
	) (result.Result[egress.TrackOk, egress.TrackError], fx.Effects, error) {
		nodeDID := inv.Issuer().DID()
		receipts := cap.Nb().Receipts
		endpoint := cap.Nb().Endpoint

		err := svc.Record(ctx, nodeDID, receipts, endpoint)
		if err != nil {
			return result.Error[egress.TrackOk, egress.TrackError](egress.NewTrackError(err.Error())), nil, nil
		}

		// produce space/egress/consolidate effect by invoking on the service itself
		consolidateInv, err := egress.Consolidate.Invoke(
			ictx.ID(),
			ictx.ID(),
			ictx.ID().DID().String(),
			egress.ConsolidateCaveats{
				Cause: inv.Link(),
			},
			delegation.WithNoExpiration(),
		)
		if err != nil {
			return result.Error[egress.TrackOk, egress.TrackError](egress.NewTrackError(err.Error())), nil, nil
		}

		effects := fx.NewEffects(fx.WithFork(fx.FromInvocation(consolidateInv)))

		return result.Ok[egress.TrackOk, egress.TrackError](egress.TrackOk{}), effects, nil
	}
}
