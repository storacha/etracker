package consolidated

import (
	"context"

	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type ConsolidatedRecord struct {
	NodeDID     did.DID
	Cause       ucan.Link
	TotalBytes  uint64
	Receipt     receipt.AnyReceipt
	ProcessedAt string
}

type ConsolidatedTable interface {
	Add(ctx context.Context, nodeDID did.DID, cause ucan.Link, bytes uint64, rcpt receipt.AnyReceipt) error
	Get(ctx context.Context, nodeDID did.DID, cause ucan.Link) (*ConsolidatedRecord, error)
	GetByNode(ctx context.Context, nodeDID did.DID) ([]ConsolidatedRecord, error)
}
