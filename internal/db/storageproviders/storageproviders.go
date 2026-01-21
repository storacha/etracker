package storageproviders

import (
	"context"
	"errors"

	"github.com/storacha/go-ucanto/did"
)

type StorageProviderRecord struct {
	Provider      did.DID
	WalletAddress string
	OperatorEmail string
	Endpoint      string
}

type GetAllResult struct {
	Records   []StorageProviderRecord
	NextToken *string
}

var ErrNotFound = errors.New("storage provider not found")

type StorageProviderTable interface {
	Get(ctx context.Context, provider did.DID) (*StorageProviderRecord, error)
	GetAll(ctx context.Context, limit int, startToken *string) (*GetAllResult, error)
}
