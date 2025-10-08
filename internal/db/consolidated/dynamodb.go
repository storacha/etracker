package consolidated

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	capegress "github.com/storacha/go-libstoracha/capabilities/space/egress"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

var _ ConsolidatedTable = (*DynamoConsolidatedTable)(nil)

type DynamoConsolidatedTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoConsolidatedTable(client *dynamodb.Client, tableName string) *DynamoConsolidatedTable {
	return &DynamoConsolidatedTable{client, tableName}
}

func (d *DynamoConsolidatedTable) Add(ctx context.Context, cause ucan.Link, node did.DID, totalEgress uint64, rcpt capegress.ConsolidateReceipt) error {
	record, err := newConsolidatedRecord(cause, node, totalEgress, rcpt)
	if err != nil {
		return fmt.Errorf("creating consolidated record: %w", err)
	}

	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return fmt.Errorf("serializing consolidated record: %w", err)
	}

	_, err = d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("storing consolidated record: %w", err)
	}

	return nil
}

func (d *DynamoConsolidatedTable) Get(ctx context.Context, cause ucan.Link) (*ConsolidatedRecord, error) {
	result, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"cause": &types.AttributeValueMemberS{Value: cause.String()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting consolidated record: %w", err)
	}

	if result.Item == nil {
		return nil, ErrNotFound
	}

	return d.unmarshalRecord(result.Item)
}

type consolidatedRecord struct {
	Cause       string    `dynamodbav:"cause"`
	Node        string    `dynamodbav:"node"`
	TotalEgress uint64    `dynamodbav:"totalEgress"`
	Receipt     []byte    `dynamodbav:"receipt"`
	ProcessedAt time.Time `dynamodbav:"processedAt"`
}

func newConsolidatedRecord(cause ucan.Link, node did.DID, totalEgress uint64, rcpt capegress.ConsolidateReceipt) (*consolidatedRecord, error) {
	// binary values must be base64-encoded before sending them to DynamoDB
	arch := rcpt.Archive()
	archBytes, err := io.ReadAll(arch)
	if err != nil {
		return nil, fmt.Errorf("reading receipt archive: %w", err)
	}

	rcptBytes := make([]byte, base64.StdEncoding.EncodedLen(len(archBytes)))
	base64.StdEncoding.Encode(rcptBytes, archBytes)

	return &consolidatedRecord{
		Cause:       cause.String(),
		Node:        node.String(),
		TotalEgress: totalEgress,
		Receipt:     rcptBytes,
		ProcessedAt: time.Now().UTC(),
	}, nil
}

func (d *DynamoConsolidatedTable) unmarshalRecord(item map[string]types.AttributeValue) (*ConsolidatedRecord, error) {
	var record consolidatedRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling consolidated record: %w", err)
	}

	node, err := did.Parse(record.Node)
	if err != nil {
		return nil, fmt.Errorf("parsing node DID: %w", err)
	}

	c, err := cid.Decode(record.Cause)
	if err != nil {
		return nil, fmt.Errorf("parsing cause CID: %w", err)
	}
	cause := cidlink.Link{Cid: c}

	archBytes := make([]byte, base64.StdEncoding.DecodedLen(len(record.Receipt)))
	if _, err := base64.StdEncoding.Decode(archBytes, record.Receipt); err != nil {
		return nil, fmt.Errorf("decoding receipt archive: %w", err)
	}

	rcpt, err := receipt.Extract(archBytes)
	if err != nil {
		return nil, fmt.Errorf("extracting receipt: %w", err)
	}

	return &ConsolidatedRecord{
		Node:        node,
		Cause:       cause,
		TotalEgress: record.TotalEgress,
		Receipt:     rcpt,
		ProcessedAt: record.ProcessedAt,
	}, nil
}
