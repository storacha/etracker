package egress

import (
	"context"
	"net/url"
	"time"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type EgressRecord struct {
	Batch      ucan.Link
	Node       did.DID
	Endpoint   string
	Cause      invocation.Invocation
	ReceivedAt time.Time
}

type EgressTable interface {
	Record(ctx context.Context, batch ucan.Link, node did.DID, endpoint *url.URL, cause invocation.Invocation) error
	GetUnprocessed(ctx context.Context, limit int) ([]EgressRecord, error)
	MarkAsProcessed(ctx context.Context, records []EgressRecord) error
	CountUnprocessedBatches(ctx context.Context) (int64, error)
}
