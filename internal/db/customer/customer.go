package customer

import (
	"context"
	"errors"

	"github.com/storacha/go-ucanto/did"
)

var ErrNotFound = errors.New("customer not found")

type ListResult struct {
	Customers []did.DID
	Cursor    *string
}

type CustomerTable interface {
	List(ctx context.Context, limit int, cursor *string) (*ListResult, error)
	Has(ctx context.Context, customerDID did.DID) (bool, error)
}
