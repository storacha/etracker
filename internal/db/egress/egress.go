package egress

import (
	"context"
	"net/url"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type EgressTable interface {
	Record(ctx context.Context, nodeID did.DID, receipt ucan.Link, endpoint *url.URL) error
}
