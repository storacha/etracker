package consumer

import (
	"context"

	"github.com/storacha/go-ucanto/did"
)

type ConsumerTable interface {
	ListByCustomer(ctx context.Context, customerID did.DID) ([]did.DID, error)
}
