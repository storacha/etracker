package consolidated

import (
	"context"
	"errors"
	"time"

	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type ConsolidatedRecord struct {
	Node        did.DID
	Cause       ucan.Link
	TotalBytes  uint64
	Receipt     receipt.AnyReceipt
	ProcessedAt time.Time
}

var ErrNotFound = errors.New("record not found")

type ConsolidatedTable interface {
	Add(ctx context.Context, node did.DID, cause ucan.Link, bytes uint64, rcpt receipt.AnyReceipt) error
	Get(ctx context.Context, node did.DID, cause ucan.Link) (*ConsolidatedRecord, error)
	GetByNode(ctx context.Context, node did.DID) ([]ConsolidatedRecord, error)
}
