package storageproviders

import (
	"context"
	"errors"

	"github.com/storacha/go-ucanto/did"
)

type StorageProviderRecord struct {
	Provider      did.DID
	OperatorEmail string
	Endpoint      string
}

var ErrNotFound = errors.New("storage provider not found")

type StorageProviderTable interface {
	Get(ctx context.Context, provider did.DID) (*StorageProviderRecord, error)
	GetAll(ctx context.Context) ([]StorageProviderRecord, error)
}
