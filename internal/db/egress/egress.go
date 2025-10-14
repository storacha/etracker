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
	PK         string
	SK         string
	Node       did.DID
	Receipts   ucan.Link
	Endpoint   string
	Cause      invocation.Invocation
	ReceivedAt time.Time
	Processed  bool
}

type EgressTable interface {
	Record(ctx context.Context, node did.DID, receipt ucan.Link, endpoint *url.URL, cause invocation.Invocation) error
	GetUnprocessed(ctx context.Context, limit int) ([]EgressRecord, error)
	MarkAsProcessed(ctx context.Context, records []EgressRecord) error
}
