package consumer

import (
	"context"

	"github.com/storacha/go-ucanto/did"
)

type Consumer struct {
	ID           did.DID
	Provider     did.DID
	Subscription string
}

type ConsumerTable interface {
	Get(ctx context.Context, consumerID string) (Consumer, error)
	ListByCustomer(ctx context.Context, customerID did.DID) ([]did.DID, error)
}
