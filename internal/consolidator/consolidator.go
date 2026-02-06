package consolidator

import (
	"context"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"slices"
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
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	ucanto "github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/storacha/etracker/internal/db/consolidated"
	"github.com/storacha/etracker/internal/db/consumer"
	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/db/spacestats"
	"github.com/storacha/etracker/internal/metrics"
)

var log = logging.Logger("consolidator")

var ErrNotFound = consolidated.ErrNotFound

type Consolidator struct {
	id                    principal.Signer
	environment           string
	egressTable           egress.EgressTable
	consolidatedTable     consolidated.ConsolidatedTable
	spaceStatsTable       spacestats.SpaceStatsTable
	consumerTable         consumer.ConsumerTable
	knownProviders        []string
	ucantoSrv             ucanto.ServerView[ucanto.Service]
	retrieveValidationCtx validator.ValidationContext[content.RetrieveCaveats]
	httpClient            *http.Client
	interval              time.Duration
	batchSize             int
	stopCh                chan struct{}
}

func New(
	id principal.Signer,
	environment string,
	egressTable egress.EgressTable,
	consolidatedTable consolidated.ConsolidatedTable,
	spaceStatsTable spacestats.SpaceStatsTable,
	consumerTable consumer.ConsumerTable,
	knownProviders []string,
	interval time.Duration,
	batchSize int,
	presolver validator.PrincipalResolverFunc,
	authProofs []delegation.Delegation,
) (*Consolidator, error) {
	retrieveValidationCtx := validator.NewValidationContext(
		id.Verifier(),
		content.Retrieve,
		validator.IsSelfIssued,
		func(context.Context, validator.Authorization[any]) validator.Revoked {
			return nil
		},
		validator.ProofUnavailable,
		verifier.Parse,
		presolver,
		// ignore expiration and not valid before
		func(dlg delegation.Delegation) validator.InvalidProof {
			return nil
		},
		authProofs...,
	)

	c := &Consolidator{
		id:                    id,
		environment:           environment,
		egressTable:           egressTable,
		consolidatedTable:     consolidatedTable,
		spaceStatsTable:       spaceStatsTable,
		consumerTable:         consumerTable,
		knownProviders:        knownProviders,
		retrieveValidationCtx: retrieveValidationCtx,
		httpClient:            &http.Client{Timeout: 30 * time.Second},
		interval:              interval,
		batchSize:             batchSize,
		stopCh:                make(chan struct{}),
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

	// Environment attribute for metrics
	envAttr := attribute.String("env", c.environment)

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
	successfulRecords := make([]egress.EgressRecord, 0, len(records))
	for _, record := range records {
		var rcpt capegress.ConsolidateReceipt
		totalEgress := uint64(0)

		bLog := log.With("node", record.Node, "batch", record.Batch.String())

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
			bLog.Errorf("executing consolidation invocation: %v", err)

			rcpt, err = c.issueErrorReceipt(consolidateInv, capegress.NewConsolidateError(err.Error()))
			if err != nil {
				bLog.Errorf("issuing error receipt: %v", err)
				continue
			}
		}

		o, x := result.Unwrap(rcpt.Out())
		var emptyErr capegress.ConsolidateError
		if x != emptyErr {
			bLog.Errorf("consolidation error: %s", x.Message)
		} else {
			totalEgress = o.TotalEgress
		}

		// Store consolidated record (one per batch)
		if err := c.consolidatedTable.Add(ctx, consolidateInv.Link(), record.Node, totalEgress, rcpt); err != nil {
			bLog.Errorf("Failed to add consolidated record: %v", err)
			continue
		}

		successfulRecords = append(successfulRecords, record)

		// Increment consolidated bytes counter for this node
		nodeAttr := attribute.String("node", record.Node.String())
		metrics.ConsolidatedBytesPerNode.Add(ctx, int64(totalEgress), metric.WithAttributeSet(attribute.NewSet(nodeAttr, envAttr)))

		bLog.Infof("Consolidated %d bytes", totalEgress)
	}

	// Mark records as processed
	if err := c.egressTable.MarkAsProcessed(ctx, successfulRecords); err != nil {
		return fmt.Errorf("marking records as processed: %w", err)
	}

	metrics.UnprocessedBatches.Add(ctx, int64(-len(successfulRecords)), metric.WithAttributeSet(attribute.NewSet(envAttr)))

	log.Infof("Consolidation cycle completed. Processed %d records (%d successful)", len(records), len(successfulRecords))

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

	// requesterNode is the node that requested a receipt batch to be tracked for egress
	requesterNode := trackInv.Issuer().DID()

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

		cap, err := validateRetrievalReceipt(ctx, requesterNode, rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
		if err != nil {
			log.Warnf("Invalid receipt: %v", err)
			continue
		}

		space, size, err := extractProperties(cap)
		if err != nil {
			log.Warnf("Failed to extract size from receipt: %v", err)
			continue
		}

		// Record space stats
		if err := c.spaceStatsTable.Record(ctx, space, size); err != nil {
			log.Errorf("Failed to record space stats: %v", err)
			// Continue processing even if stats recording fails
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// a receipt batch is a flat CAR file where each block is an archived receipt
	_, blks, err := car.Decode(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decoding receipt batch: %w", err)
	}

	return func(yield func(receipt.AnyReceipt, error) bool) {
		defer resp.Body.Close()

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

func validateRetrievalReceipt(
	ctx context.Context,
	requesterNode did.DID,
	rcpt receipt.AnyReceipt,
	validationCtx validator.ValidationContext[content.RetrieveCaveats],
	consumerTable consumer.ConsumerTable,
	knownProviders []string,
) (ucan.Capability[content.RetrieveCaveats], error) {
	// Confirm the receipt is not a failure receipt
	_, x := result.Unwrap(rcpt.Out())
	if x != nil {
		return nil, fmt.Errorf("receipt is a failure receipt")
	}

	r, err := receipt.Rebind[content.RetrieveOk, fdm.FailureModel](rcpt, content.RetrieveOkType(), fdm.FailureType())
	if err != nil {
		return nil, fmt.Errorf("receipt is not a space/content/retrieve receipt: %w", err)
	}

	// Confirm the receipt is issued by the node that submitted the batch for egress tracking
	if r.Issuer().DID() != requesterNode {
		return nil, fmt.Errorf("receipt is not issued by the requester node")
	}

	// Verify receipt's signature
	reqNodeVerifier, err := verifier.Parse(requesterNode.String())
	if err != nil {
		return nil, fmt.Errorf("parsing requester node key: %w", err)
	}

	verified, err := r.VerifySignature(reqNodeVerifier)
	if err != nil {
		return nil, fmt.Errorf("verifying receipt signature: %w", err)
	}
	if !verified {
		return nil, fmt.Errorf("receipt signature is invalid")
	}

	// Confirm the receipt is for a `space/content/retrieve` invocation
	inv, ok := r.Ran().Invocation()
	if !ok {
		return nil, fmt.Errorf("original retrieve invocation must be attached to the receipt")
	}

	if len(inv.Capabilities()) != 1 {
		return nil, fmt.Errorf("expected exactly one capability in the invocation")
	}

	cap := inv.Capabilities()[0]
	if cap.Can() != content.RetrieveAbility {
		return nil, fmt.Errorf("original invocation is not a %s invocation, but a %s one", content.RetrieveAbility, cap.Can())
	}

	// Check the space has been provisioned by the upload service
	space := cap.With()
	consumer, err := consumerTable.Get(ctx, space)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer: %w", err)
	}
	if !slices.Contains(knownProviders, consumer.Provider.String()) {
		return nil, fmt.Errorf("unknown space provider %s", consumer.Provider)
	}

	// Verify the delegation chain
	auth, verr := validator.Access(ctx, inv, validationCtx)
	if verr != nil {
		return nil, fmt.Errorf("invalid delegation chain: %w", verr)
	}

	return auth.Capability(), nil
}

func extractProperties(cap ucan.Capability[content.RetrieveCaveats]) (did.DID, uint64, error) {
	space, err := did.Parse(string(cap.With()))
	if err != nil {
		return did.Undef, 0, fmt.Errorf("parsing space from with %s: %w", cap.With(), err)
	}

	size := cap.Nb().Range.End - cap.Nb().Range.Start + 1

	return space, size, nil
}

func (c *Consolidator) GetReceipt(ctx context.Context, cause ucan.Link) (receipt.AnyReceipt, error) {
	consRecord, err := c.consolidatedTable.Get(ctx, cause)
	if err != nil {
		return nil, err
	}

	return consRecord.Receipt, nil
}
