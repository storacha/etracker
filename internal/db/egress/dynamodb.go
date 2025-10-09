package egress

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

func (d *DynamoEgressTable) Record(ctx context.Context, batch ucan.Link, node did.DID, endpoint *url.URL, cause invocation.Invocation) error {
	record, err := newRecord(batch, node, endpoint, cause)
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
	result, err := d.client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(d.tableName),
		FilterExpression: aws.String("proc = :false"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":false": &types.AttributeValueMemberBOOL{Value: false},
		},
		Limit: aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, fmt.Errorf("querying unprocessed records: %w", err)
	}

	var unprocessed []EgressRecord
	for _, item := range result.Items {
		record, err := d.unmarshalRecord(item)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling egress record: %w", err)
		}

		unprocessed = append(unprocessed, *record)
	}

	return unprocessed, nil
}

type egressRecord struct {
	Batch      string `dynamodbav:"batch"`
	Node       string `dynamodbav:"node"`
	Endpoint   string `dynamodbav:"endpoint"`
	Cause      []byte `dynamodbav:"cause"`
	ReceivedAt string `dynamodbav:"receivedAt"`
	Processed  bool   `dynamodbav:"proc"`
}

func newRecord(batch ucan.Link, node did.DID, endpoint *url.URL, cause invocation.Invocation) (*egressRecord, error) {
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
		Batch:      batch.String(),
		Node:       node.String(),
		Endpoint:   endpointStr,
		Cause:      causeBytes,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		Processed:  false,
	}, nil
}

func (d *DynamoEgressTable) unmarshalRecord(item map[string]types.AttributeValue) (*EgressRecord, error) {
	var record egressRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling egress record: %w", err)
	}

	node, err := did.Parse(record.Node)
	if err != nil {
		return nil, fmt.Errorf("parsing node DID: %w", err)
	}

	c, err := cid.Decode(record.Batch)
	if err != nil {
		return nil, fmt.Errorf("parsing batch CID: %w", err)
	}
	batch := cidlink.Link{Cid: c}

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
		Batch:      batch,
		Node:       node,
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
				"batch": &types.AttributeValueMemberS{Value: record.Batch.String()},
			},
			UpdateExpression: aws.String("SET proc = :true"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":true": &types.AttributeValueMemberBOOL{Value: true},
			},
		})
		if err != nil {
			return fmt.Errorf("marking record as processed (batch=%s): %w", record.Batch.String(), err)
		}
	}
	return nil
}
