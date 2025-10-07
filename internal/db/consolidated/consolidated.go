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
	Cause       ucan.Link
	Node        did.DID
	TotalEgress uint64
	Receipt     receipt.AnyReceipt
	ProcessedAt time.Time
}

var ErrNotFound = errors.New("record not found")

type ConsolidatedTable interface {
	Add(ctx context.Context, cause ucan.Link, node did.DID, totalEgress uint64, rcpt receipt.AnyReceipt) error
	Get(ctx context.Context, cause ucan.Link) (*ConsolidatedRecord, error)
}
