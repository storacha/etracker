package egress

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

var _ EgressTable = (*DynamoEgressTable)(nil)

type DynamoEgressTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoEgressTable(client *dynamodb.Client, tableName string) *DynamoEgressTable {
	return &DynamoEgressTable{client, tableName}
}

func (d *DynamoEgressTable) Record(ctx context.Context, nodeID did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) error {
	record, err := newRecord(nodeID, receipts, endpoint, cause)
	if err != nil {
		return fmt.Errorf("creating egress record: %w", err)
	}

	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return fmt.Errorf("serializing egress record: %w", err)
	}

	_, err = d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName), Item: item,
	})
	if err != nil {
		return fmt.Errorf("storing egress record: %w", err)
	}

	return nil
}

func (d *DynamoEgressTable) GetUnprocessed(ctx context.Context, limit int) ([]EgressRecord, error) {
	// Scan all shards for the current date for unprocessed records
	today := time.Now().UTC().Format("2006-01-02")
	var allRecords []EgressRecord

	for shard := range 10 {
		pk := fmt.Sprintf("%s#%d", today, shard)

		result, err := d.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(d.tableName),
			KeyConditionExpression: aws.String("PK = :pk"),
			FilterExpression:       aws.String("proc = :false"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":    &types.AttributeValueMemberS{Value: pk},
				":false": &types.AttributeValueMemberBOOL{Value: false},
			},
			Limit: aws.Int32(int32(limit)),
		})
		if err != nil {
			return nil, fmt.Errorf("querying unprocessed records for shard %d: %w", shard, err)
		}

		for _, item := range result.Items {
			record, err := d.unmarshalRecord(item)
			if err != nil {
				return nil, fmt.Errorf("unmarshaling egress record: %w", err)
			}

			allRecords = append(allRecords, *record)

			if len(allRecords) >= limit {
				return allRecords, nil
			}
		}
	}

	return allRecords, nil
}

type egressRecord struct {
	// Partition key: "DATE#SHARD" (e.g., "2025-08-18#0")
	// Where SHARD is a number 0-9 to distribute writes
	PK string `dynamodbav:"PK"`

	// Sort key: "RECEIVED_AT#NODE_ID#UNIQUE_ID"
	// This allows sorting by time within each date partition
	SK string `dynamodbav:"SK"`

	NodeID     string `dynamodbav:"nodeID"`
	Receipts   string `dynamodbav:"receipts"`
	Endpoint   string `dynamodbav:"endpoint"`
	Cause      []byte `dynamodbav:"cause"`
	ReceivedAt string `dynamodbav:"receivedAt"`
	Processed  bool   `dynamodbav:"proc"`
}

func newRecord(nodeID did.DID, receipts ucan.Link, endpoint *url.URL, cause invocation.Invocation) (*egressRecord, error) {
	// TODO: review keys to improve performance and access patterns
	receivedAt := time.Now().UTC()
	dateStr := receivedAt.Format("2006-01-02")
	shard := rand.Intn(10)
	pk := fmt.Sprintf("%s#%d", dateStr, shard)
	sk := fmt.Sprintf("%s#%s#%s", dateStr, nodeID, uuid.New())
	endpointStr, _ := url.PathUnescape(endpoint.String())

	// binary values must be base64-encoded before sending them to DynamoDB
	arch := cause.Archive()
	archBytes, err := io.ReadAll(arch)
	if err != nil {
		return nil, fmt.Errorf("reading invocation archive: %w", err)
	}

	causeBytes := make([]byte, base64.StdEncoding.EncodedLen(len(archBytes)))
	base64.StdEncoding.Encode(causeBytes, archBytes)

	return &egressRecord{
		PK:         pk,
		SK:         sk,
		NodeID:     nodeID.String(),
		Receipts:   receipts.String(),
		Endpoint:   endpointStr,
		Cause:      causeBytes,
		ReceivedAt: receivedAt.Format(time.RFC3339),
		Processed:  false,
	}, nil
}

func (d *DynamoEgressTable) unmarshalRecord(item map[string]types.AttributeValue) (*EgressRecord, error) {
	var record egressRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling egress record: %w", err)
	}

	nodeID, err := did.Parse(record.NodeID)
	if err != nil {
		return nil, fmt.Errorf("parsing node DID: %w", err)
	}

	c, err := cid.Decode(record.Receipts)
	if err != nil {
		return nil, fmt.Errorf("parsing receipts CID: %w", err)
	}
	receipts := cidlink.Link{Cid: c}

	archBytes := make([]byte, base64.StdEncoding.DecodedLen(len(record.Cause)))
	if _, err := base64.StdEncoding.Decode(archBytes, record.Cause); err != nil {
		return nil, fmt.Errorf("decoding cause archive: %w", err)
	}

	cause, err := delegation.Extract(archBytes)
	if err != nil {
		return nil, fmt.Errorf("extracting cause: %w", err)
	}

	receivedAt, err := time.Parse(time.RFC3339, record.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("parsing received at time: %w", err)
	}

	return &EgressRecord{
		PK:         record.PK,
		SK:         record.SK,
		NodeID:     nodeID,
		Receipts:   receipts,
		Endpoint:   record.Endpoint,
		Cause:      cause,
		ReceivedAt: receivedAt,
		Processed:  record.Processed,
	}, nil
}

func (d *DynamoEgressTable) MarkAsProcessed(ctx context.Context, records []EgressRecord) error {
	for _, record := range records {
		_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(d.tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: record.PK},
				"SK": &types.AttributeValueMemberS{Value: record.SK},
			},
			UpdateExpression: aws.String("SET proc = :true"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":true": &types.AttributeValueMemberBOOL{Value: true},
			},
		})
		if err != nil {
			return fmt.Errorf("marking record as processed (PK=%s, SK=%s): %w", record.PK, record.SK, err)
		}
	}
	return nil
}
