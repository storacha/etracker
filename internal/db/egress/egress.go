package egress

import (
	"context"
	"net/url"
	"time"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type EgressRecord struct {
	PK         string
	SK         string
	NodeID     did.DID
	Receipts   ucan.Link
	Endpoint   *url.URL
	Cause      ucan.Link
	ReceivedAt time.Time
	Processed  bool
}

type EgressTable interface {
	Record(ctx context.Context, nodeID did.DID, receipt ucan.Link, endpoint *url.URL, cause ucan.Link) error
	GetUnprocessed(ctx context.Context, limit int) ([]EgressRecord, error)
	MarkAsProcessed(ctx context.Context, records []EgressRecord) error
}
