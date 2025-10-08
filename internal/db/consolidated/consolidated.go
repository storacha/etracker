package consolidated

import (
	"context"
	"iter"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type ConsolidatedRecord struct {
	NodeDID          did.DID
	ReceiptsBatchCID ucan.Link
	TotalBytes       uint64
	ProcessedAt      string
}

type ConsolidatedTable interface {
	Add(ctx context.Context, nodeDID did.DID, receiptsBatchCID ucan.Link, bytes uint64) error
	Get(ctx context.Context, nodeDID did.DID, receiptsBatchCID ucan.Link) (ConsolidatedRecord, error)
	List(ctx context.Context, nodeDID did.DID) (iter.Seq2[ConsolidatedRecord, error], error)
}
