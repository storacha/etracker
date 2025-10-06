package consolidator

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	capegress "github.com/storacha/go-libstoracha/capabilities/space/egress"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/db/consolidated"
	"github.com/storacha/etracker/internal/db/egress"
)

var log = logging.Logger("consolidator")

var ErrNotFound = consolidated.ErrNotFound

type Consolidator struct {
	id                principal.Signer
	egressTable       egress.EgressTable
	consolidatedTable consolidated.ConsolidatedTable
	httpClient        *http.Client
	interval          time.Duration
	batchSize         int
	stopCh            chan struct{}
}

func New(id principal.Signer, egressTable egress.EgressTable, consolidatedTable consolidated.ConsolidatedTable, interval time.Duration, batchSize int) *Consolidator {
	return &Consolidator{
		id:                id,
		egressTable:       egressTable,
		consolidatedTable: consolidatedTable,
		httpClient:        &http.Client{Timeout: 30 * time.Second},
		interval:          interval,
		batchSize:         batchSize,
		stopCh:            make(chan struct{}),
	}
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
		log.Debug("No unprocessed records found")
		return nil
	}

	log.Infof("Processing %d unprocessed records", len(records))

	// Process each record (each record represents a batch of receipts for a single node)
	for _, record := range records {
		// According to the spec, consolidation happens as a result of a `space/egress/consolidate` invocation.
		// Since the service is invoking it on itself, we will generate it here.
		// Is this acceptable or should we create and register a handler and follow the full go-ucanto flow instead?
		inv, err := capegress.Consolidate.Invoke(
			c.id,
			c.id,
			c.id.DID().String(),
			capegress.ConsolidateCaveats{
				Cause: record.Cause,
			},
			delegation.WithNoExpiration(),
		)
		if err != nil {
			log.Errorf("generating consolidation invocation: %w", err)
			continue
		}

		// Fetch receipts from the endpoint
		receipts, err := c.fetchReceipts(ctx, record)
		if err != nil {
			log.Errorf("Failed to fetch receipts for record (nodeID=%s): %v", record.NodeID, err)
			continue
		}

		// Process each receipt in the batch
		totalBytes := uint64(0)
		for _, rcpt := range receipts {
			retrievalRcpt, err := receipt.Rebind[content.RetrieveOk, fdm.FailureModel](rcpt, content.RetrieveOkType(), fdm.FailureType())
			if err != nil {
				log.Warnf("receipt doesn't seem a retrieval receipt: %w", err)
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

			totalBytes += size
		}

		// Issue the receipt for the consolidation operation
		// TODO: store in the DB
		consolidationRcpt, err := receipt.Issue(
			c.id,
			result.Ok[capegress.ConsolidateOk, capegress.ConsolidateError](capegress.ConsolidateOk{}),
			ran.FromInvocation(inv),
		)
		if err != nil {
			log.Errorf("Failed to issue consolidation receipt: %v", err)
			continue
		}

		// Store consolidated record (one per batch)
		if err := c.consolidatedTable.Add(ctx, record.NodeID, record.Receipts, totalBytes, consolidationRcpt); err != nil {
			log.Errorf("Failed to add consolidated record for node %s, batch %s: %v", record.NodeID, record.Receipts, err)
			continue
		}

		log.Infof("Consolidated %d bytes for node %s (batch %s)", totalBytes, record.NodeID, record.Receipts)
	}

	// Mark records as processed
	if err := c.egressTable.MarkAsProcessed(ctx, records); err != nil {
		return fmt.Errorf("marking records as processed: %w", err)
	}

	log.Infof("Consolidation cycle completed. Processed %d records", len(records))

	return nil
}

func (c *Consolidator) fetchReceipts(ctx context.Context, record egress.EgressRecord) ([]receipt.AnyReceipt, error) {
	// Substitute {cid} in the endpoint URL with the receipts CID
	batchURLStr := record.Endpoint
	batchCID := record.Receipts.String()

	// Handle both {cid} and :cid patterns
	batchURLStr = strings.ReplaceAll(batchURLStr, "{cid}", batchCID)
	batchURLStr = strings.ReplaceAll(batchURLStr, ":cid", batchCID)

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
	defer resp.Body.Close()

	var rcpts []receipt.AnyReceipt
	for blk, err := range blks {
		if err != nil {
			return nil, fmt.Errorf("iterating over receipt blocks: %w", err)
		}

		rcpt, err := receipt.Extract(blk.Bytes())
		if err != nil {
			return nil, fmt.Errorf("extracting receipt: %w", err)
		}

		rcpts = append(rcpts, rcpt)
	}

	return rcpts, nil
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

func (c *Consolidator) GetReceipt(ctx context.Context, nodeDID did.DID, cause ucan.Link) (receipt.AnyReceipt, error) {
	consRecord, err := c.consolidatedTable.Get(ctx, nodeDID, cause)
	if err != nil {
		return nil, err
	}

	return consRecord.Receipt, nil
}
