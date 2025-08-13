package ucanserver

import (
	"context"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	userver "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/payme/internal/capabilities"
)

var serviceMethods = []userver.Option{
	userver.WithServiceMethod(
		capabilities.PayMeAbility,
		userver.Provide(capabilities.PayMe, ucanPayMeHandler),
	),
}

func ucanPayMeHandler(ctx context.Context, cap ucan.Capability[capabilities.PayMeCaveats], inv invocation.Invocation, ictx userver.InvocationContext) (capabilities.PayMeOk, fx.Effects, error) {

	return capabilities.PayMeOk{}, nil, nil
}
