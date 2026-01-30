package server

import (
	"context"
	"errors"

	accountegress "github.com/storacha/go-libstoracha/capabilities/account/egress"
	"github.com/storacha/go-libstoracha/capabilities/space/egress"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	userver "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/service"
)

func serviceMethods(svc service.Service) []userver.Option {
	return []userver.Option{
		userver.WithServiceMethod(
			egress.TrackAbility,
			userver.Provide(egress.Track, ucanTrackHandler(svc)),
		),
		userver.WithServiceMethod(
			accountegress.GetAbility,
			userver.Provide(accountegress.Get, ucanAccountEgressGetHandler(svc)),
		),
	}
}

func ucanTrackHandler(svc service.Service) func(
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
		node := inv.Issuer().DID()
		receipts := cap.Nb().Receipts
		endpoint := cap.Nb().Endpoint

		err := svc.Record(ctx, node, receipts, endpoint, inv)
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

func ucanAccountEgressGetHandler(svc service.Service) func(
	ctx context.Context,
	cap ucan.Capability[accountegress.GetCaveats],
	inv invocation.Invocation,
	ictx userver.InvocationContext,
) (result.Result[accountegress.GetOk, accountegress.GetError], fx.Effects, error) {
	return func(
		ctx context.Context,
		cap ucan.Capability[accountegress.GetCaveats],
		inv invocation.Invocation,
		ictx userver.InvocationContext,
	) (result.Result[accountegress.GetOk, accountegress.GetError], fx.Effects, error) {
		// 1. Extract account DID from capability subject
		accountDID, err := did.Parse(cap.With())
		if err != nil {
			return nil, nil, err
		}

		// 2. Extract filters from caveats
		caveats := cap.Nb()
		spacesFilter := caveats.Spaces
		var periodFilter *service.Period
		if caveats.Period != nil {
			periodFilter = &service.Period{
				From: caveats.Period.From,
				To:   caveats.Period.To,
			}
		}

		// 3. Call service layer
		egressData, err := svc.GetAccountEgress(ctx, accountDID, spacesFilter, periodFilter)
		if err != nil {
			var accErr service.ErrAccountNotFound
			if errors.As(err, &accErr) {
				return result.Error[accountegress.GetOk, accountegress.GetError](
					accountegress.NewAccountNotFoundError(accErr.Error()),
				), nil, nil
			}

			var spaceErr service.ErrSpaceUnauthorized
			if errors.As(err, &spaceErr) {
				return result.Error[accountegress.GetOk, accountegress.GetError](
					accountegress.NewSpaceUnauthorizedError(spaceErr.Error()),
				), nil, nil
			}

			var periodErr service.ErrPeriodNotAcceptable
			if errors.As(err, &periodErr) {
				return result.Error[accountegress.GetOk, accountegress.GetError](
					accountegress.NewPeriodNotAcceptableError(periodErr.Error()),
				), nil, nil
			}

			// System error
			return nil, nil, err
		}

		// 4. Transform service result to GetOk format
		spacesModel := accountegress.SpacesModel{
			Keys:   make([]did.DID, 0, len(egressData.Spaces)),
			Values: make(map[did.DID]accountegress.SpaceEgress, len(egressData.Spaces)),
		}

		for spaceDID, spaceData := range egressData.Spaces {
			dailyStats := make([]accountegress.DailyStats, len(spaceData.DailyStats))
			for i, ds := range spaceData.DailyStats {
				dailyStats[i] = accountegress.DailyStats{
					Date:   ds.Date,
					Egress: ds.Egress,
				}
			}

			spacesModel.Keys = append(spacesModel.Keys, spaceDID)
			spacesModel.Values[spaceDID] = accountegress.SpaceEgress{
				Total:      spaceData.Total,
				DailyStats: dailyStats,
			}
		}

		ok := accountegress.GetOk{
			Total:  egressData.Total,
			Spaces: spacesModel,
		}

		return result.Ok[accountegress.GetOk, accountegress.GetError](ok), nil, nil
	}
}
