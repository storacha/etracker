package consolidator

import (
	"context"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	capegress "github.com/storacha/go-libstoracha/capabilities/space/egress"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/principal"
	ucanto "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/storacha/etracker/internal/db/consolidated"
	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/metrics"
)

var log = logging.Logger("consolidator")

var ErrNotFound = consolidated.ErrNotFound

type Consolidator struct {
	id                principal.Signer
	egressTable       egress.EgressTable
	consolidatedTable consolidated.ConsolidatedTable
	ucantoSrv         ucanto.ServerView[ucanto.Service]
	httpClient        *http.Client
	interval          time.Duration
	batchSize         int
	stopCh            chan struct{}
}

func New(id principal.Signer, egressTable egress.EgressTable, consolidatedTable consolidated.ConsolidatedTable, interval time.Duration, batchSize int) (*Consolidator, error) {
	c := &Consolidator{
		id:                id,
		egressTable:       egressTable,
		consolidatedTable: consolidatedTable,
		httpClient:        &http.Client{Timeout: 30 * time.Second},
		interval:          interval,
		batchSize:         batchSize,
		stopCh:            make(chan struct{}),
	}

	ucantoSrv, err := ucanto.NewServer(
		id,
		ucanto.WithServiceMethod(capegress.ConsolidateAbility, ucanto.Provide(capegress.Consolidate, c.ucanConsolidateHandler)),
	)
	if err != nil {
		return nil, err
	}

	c.ucantoSrv = ucantoSrv

	return c, nil
}

func (c *Consolidator) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	log.Infof("Consolidator started with interval: %v", c.interval)

	for {
		select {
		case <-ctx.Done():
			log.Info("Consolidator stopping due to context cancellation")
			return
		case <-c.stopCh:
			log.Info("Consolidator stopping")
			return
		case <-ticker.C:
			if err := c.Consolidate(ctx); err != nil {
				log.Errorf("Consolidation error: %v", err)
			}
		}
	}
}

func (c *Consolidator) Stop() {
	close(c.stopCh)
}

func (c *Consolidator) Consolidate(ctx context.Context) error {
	log.Info("Starting consolidation cycle")

	// Get unprocessed records
	records, err := c.egressTable.GetUnprocessed(ctx, c.batchSize)
	if err != nil {
		return fmt.Errorf("fetching unprocessed records: %w", err)
	}

	if len(records) == 0 {
		log.Info("No unprocessed records found")
		return nil
	}

	log.Infof("Processing %d unprocessed records", len(records))

	// Process each record (each record represents a batch of receipts for a single node)
	for _, record := range records {
		var rcpt capegress.ConsolidateReceipt
		totalEgress := uint64(0)

		bLog := log.With("node", record.Node, "batch", record.Receipts.String())

		// According to the spec, consolidation happens as a result of a `space/egress/consolidate` invocation.
		// We use the consolidator's own ucanto server to invoke the consolidate capability on itself.
		consolidateInv, err := capegress.Consolidate.Invoke(
			c.id,
			c.id,
			c.id.DID().String(),
			capegress.ConsolidateCaveats{
				Cause: record.Cause.Link(),
			},
			delegation.WithNoExpiration(),
		)
		if err != nil {
			bLog.Errorf("generating consolidation invocation: %v", err)
			continue
		}

		var attachErr error
		for blk, err := range record.Cause.Blocks() {
			if err != nil {
				attachErr = err
				break
			}

			if err := consolidateInv.Attach(blk); err != nil {
				attachErr = err
				break
			}
		}
		if attachErr != nil {
			bLog.Errorf("attaching blocks to consolidation invocation: %v", attachErr)
			continue
		}

		rcpt, err = c.execConsolidateInvocation(ctx, consolidateInv)
		if err != nil {
			rcpt, err = c.issueErrorReceipt(consolidateInv, capegress.NewConsolidateError(err.Error()))
			if err != nil {
				bLog.Errorf("issuing error receipt: %v", err)
				continue
			}
		}

		o, x := result.Unwrap(rcpt.Out())
		var emptyErr capegress.ConsolidateError
		if x != emptyErr {
			bLog.Errorf("invocation failed: %s", x.Message)
		} else {
			totalEgress = o.TotalEgress
		}

		// Store consolidated record (one per batch)
		if err := c.consolidatedTable.Add(ctx, consolidateInv.Link(), record.Node, totalEgress, rcpt); err != nil {
			bLog.Errorf("Failed to add consolidated record: %v", err)
			continue
		}

		// Increment consolidated bytes counter for this node
		attributes := attribute.NewSet(attribute.String("node", record.Node.String()))
		metrics.ConsolidatedBytesPerNode.Add(ctx, int64(totalEgress), metric.WithAttributeSet(attributes))

		bLog.Infof("Consolidated %d bytes", totalEgress)
	}

	// Mark records as processed
	if err := c.egressTable.MarkAsProcessed(ctx, records); err != nil {
		return fmt.Errorf("marking records as processed: %w", err)
	}

	log.Infof("Consolidation cycle completed. Processed %d records", len(records))

	return nil
}

func (c *Consolidator) execConsolidateInvocation(ctx context.Context, inv invocation.Invocation) (capegress.ConsolidateReceipt, error) {
	conn, err := client.NewConnection(c.id, c.ucantoSrv)
	if err != nil {
		return nil, fmt.Errorf("creating connection: %w", err)
	}

	resp, err := client.Execute(ctx, []invocation.Invocation{inv}, conn)
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcptLnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("missing receipt for invocation: %s", inv.Link().String())
	}

	blocks, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return nil, fmt.Errorf("importing response blocks into blockstore: %w", err)
	}

	rcptReader, err := capegress.NewConsolidateReceiptReader()
	if err != nil {
		return nil, fmt.Errorf("constructing receipt reader: %w", err)
	}

	rcpt, err := rcptReader.Read(rcptLnk, blocks.Iterator())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	return rcpt, nil
}

func (c *Consolidator) issueErrorReceipt(ranInv invocation.Invocation, failure capegress.ConsolidateError) (capegress.ConsolidateReceipt, error) {
	anyRcpt, err := receipt.Issue(
		c.id,
		result.Error[capegress.ConsolidateOk, capegress.ConsolidateError](failure),
		ran.FromInvocation(ranInv),
	)
	if err != nil {
		return nil, err
	}

	reader, err := capegress.NewConsolidateReceiptReader()
	if err != nil {
		return nil, err
	}

	return reader.Read(anyRcpt.Root().Link(), anyRcpt.Blocks())
}

func (c *Consolidator) ucanConsolidateHandler(
	ctx context.Context,
	cap ucan.Capability[capegress.ConsolidateCaveats],
	inv invocation.Invocation,
	ictx ucanto.InvocationContext,
) (result.Result[capegress.ConsolidateOk, capegress.ConsolidateError], fx.Effects, error) {
	// Fetch the original egress/track invocation from the egress/consolidate invocation
	trackInvLink := cap.Nb().Cause
	blocks, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(inv.Blocks()))
	if err != nil {
		return nil, nil, fmt.Errorf("importing invocation blocks: %w", err)
	}

	trackInv, err := invocation.NewInvocationView(trackInvLink, blocks)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching attached track invocation: %w", err)
	}

	trackCaveats, err := capegress.TrackCaveatsReader.Read(trackInv.Capabilities()[0].Nb())
	if err != nil {
		return nil, nil, fmt.Errorf("reading track caveats: %w", err)
	}

	// Fetch receipts from the endpoint
	receipts, err := c.fetchReceipts(ctx, trackCaveats.Endpoint, trackCaveats.Receipts)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching receipts: %w", err)
	}

	// Process each receipt in the batch
	totalEgress := uint64(0)
	for rcpt, err := range receipts {
		if err != nil {
			log.Errorf("Failed to fetch receipt from batch: %v", err)
			continue
		}

		retrievalRcpt, err := receipt.Rebind[content.RetrieveOk, fdm.FailureModel](rcpt, content.RetrieveOkType(), fdm.FailureType())
		if err != nil {
			log.Warnf("Receipt doesn't seem to be a retrieval receipt: %v", err)
			continue
		}

		if err := c.validateReceipt(retrievalRcpt); err != nil {
			log.Warnf("Invalid receipt: %v", err)
			continue
		}

		size, err := c.extractSize(retrievalRcpt)
		if err != nil {
			log.Warnf("Failed to extract size from receipt: %v", err)
			continue
		}

		totalEgress += size
	}

	return result.Ok[capegress.ConsolidateOk, capegress.ConsolidateError](capegress.ConsolidateOk{TotalEgress: totalEgress}), nil, nil
}

func (c *Consolidator) fetchReceipts(ctx context.Context, endpoint *url.URL, batchCID ucan.Link) (iter.Seq2[receipt.AnyReceipt, error], error) {
	// Substitute {cid} in the endpoint URL with the receipts CID
	batchURLStr, err := url.PathUnescape(endpoint.String())
	if err != nil {
		return nil, fmt.Errorf("unescaping endpoint URL: %w", err)
	}

	batchCIDStr := batchCID.String()

	// Handle both {cid} and :cid patterns
	batchURLStr = strings.ReplaceAll(batchURLStr, "{cid}", batchCIDStr)
	batchURLStr = strings.ReplaceAll(batchURLStr, ":cid", batchCIDStr)

	batchURL, err := url.Parse(batchURLStr)
	if err != nil {
		return nil, fmt.Errorf("parsing batch URL: %w", err)
	}

	log.Debugf("Fetching receipts from %s", batchURL.String())

	req, err := http.NewRequestWithContext(ctx, "GET", batchURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching receipts from %s: %w", batchURL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// a receipt batch is a flat CAR file where each block is an archived receipt
	_, blks, err := car.Decode(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decoding receipt batch: %w", err)
	}

	return func(yield func(receipt.AnyReceipt, error) bool) {
		for blk, err := range blks {
			if err != nil {
				if !yield(nil, fmt.Errorf("iterating over batch blocks: %w", err)) {
					return
				}

				continue
			}

			rcpt, err := receipt.Extract(blk.Bytes())
			if err != nil {
				if !yield(nil, fmt.Errorf("extracting receipt: %w", err)) {
					return
				}

				continue
			}

			if !yield(rcpt, nil) {
				return
			}
		}
	}, nil
}

func (c *Consolidator) validateReceipt(retrievalRcpt receipt.Receipt[content.RetrieveOk, fdm.FailureModel]) error {
	_, x := result.Unwrap(retrievalRcpt.Out())
	var emptyFailure fdm.FailureModel
	if x != emptyFailure {
		return fmt.Errorf("receipt is a failure receipt")
	}

	// TODO: do more validation here.
	// At the very least that the invocation is a retrieval invocation and the audience is the node

	return nil
}

func (c *Consolidator) extractSize(retrievalRcpt receipt.Receipt[content.RetrieveOk, fdm.FailureModel]) (uint64, error) {
	_, x := result.Unwrap(retrievalRcpt.Out())
	var emptyFailure fdm.FailureModel
	if x != emptyFailure {
		return 0, fmt.Errorf("receipt is a failure receipt")
	}

	inv, ok := retrievalRcpt.Ran().Invocation()
	if !ok {
		return 0, fmt.Errorf("expected the ran invocation to be attached to the receipt")
	}

	caps := inv.Capabilities()
	if len(caps) != 1 {
		return 0, fmt.Errorf("expected exactly one capability in the invocation")
	}

	cap := caps[0]
	if cap.Can() != content.RetrieveAbility {
		return 0, fmt.Errorf("original invocation is not a retrieval invocation, but a %s", cap.Can())
	}

	caveats, err := content.RetrieveCaveatsReader.Read(cap.Nb())
	if err != nil {
		return 0, fmt.Errorf("reading caveats from invocation: %w", err)
	}

	return caveats.Range.End - caveats.Range.Start + 1, nil
}

func (c *Consolidator) GetReceipt(ctx context.Context, cause ucan.Link) (receipt.AnyReceipt, error) {
	consRecord, err := c.consolidatedTable.Get(ctx, cause)
	if err != nil {
		return nil, err
	}

	return consRecord.Receipt, nil
}
