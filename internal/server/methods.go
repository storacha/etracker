package server

import (
	"context"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	userver "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/capabilities"
	"github.com/storacha/etracker/internal/service"
)

func serviceMethods(svc *service.Service) []userver.Option {
	return []userver.Option{
		userver.WithServiceMethod(
			capabilities.PayMeAbility,
			userver.Provide(capabilities.PayMe, ucanPayMeHandler(svc)),
		),
	}
}

func ucanPayMeHandler(svc *service.Service) func(ctx context.Context, cap ucan.Capability[capabilities.PayMeCaveats], inv invocation.Invocation, ictx userver.InvocationContext) (capabilities.PayMeOk, fx.Effects, error) {
	return func(ctx context.Context, cap ucan.Capability[capabilities.PayMeCaveats], inv invocation.Invocation, ictx userver.InvocationContext) (capabilities.PayMeOk, fx.Effects, error) {
		nodeDID := inv.Issuer().DID()
		receipts := cap.Nb().Receipts
		endpoint := cap.Nb().Endpoint

		err := svc.Record(ctx, nodeDID, receipts, endpoint)
		if err != nil {
			return capabilities.PayMeOk{}, nil, err
		}

		return capabilities.PayMeOk{}, nil, nil
	}
}
