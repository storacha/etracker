package consolidated

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
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

type consolidatedRecord struct {
	NodeDID          string `dynamodbav:"NodeDID"`
	ReceiptsBatchCID string `dynamodbav:"ReceiptsBatchCID"`
	TotalBytes       uint64 `dynamodbav:"TotalBytes"`
	ProcessedAt      string `dynamodbav:"ProcessedAt"`
}

func (d *DynamoConsolidatedTable) Add(ctx context.Context, nodeDID did.DID, receiptsBatchCID ucan.Link, bytes uint64) error {
	record := consolidatedRecord{
		NodeDID:          nodeDID.String(),
		ReceiptsBatchCID: receiptsBatchCID.String(),
		TotalBytes:       bytes,
		ProcessedAt:      time.Now().UTC().Format(time.RFC3339),
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

func (d *DynamoConsolidatedTable) Get(ctx context.Context, nodeDID did.DID, receiptsBatchCID ucan.Link) (*ConsolidatedRecord, error) {
	result, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"NodeDID":          &types.AttributeValueMemberS{Value: nodeDID.String()},
			"ReceiptsBatchCID": &types.AttributeValueMemberS{Value: receiptsBatchCID.String()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("getting consolidated record: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("record not found")
	}

	return d.unmarshalRecord(result.Item)
}

func (d *DynamoConsolidatedTable) GetByNode(ctx context.Context, nodeDID did.DID) ([]ConsolidatedRecord, error) {
	result, err := d.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("NodeDID = :nodeDID"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nodeDID": &types.AttributeValueMemberS{Value: nodeDID.String()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("querying consolidated records by node: %w", err)
	}

	records := make([]ConsolidatedRecord, 0, len(result.Items))
	for _, item := range result.Items {
		record, err := d.unmarshalRecord(item)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}

	return records, nil
}

func (d *DynamoConsolidatedTable) unmarshalRecord(item map[string]types.AttributeValue) (*ConsolidatedRecord, error) {
	var record consolidatedRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling consolidated record: %w", err)
	}

	parsedDID, err := did.Parse(record.NodeDID)
	if err != nil {
		return nil, fmt.Errorf("parsing node DID: %w", err)
	}

	c, err := cid.Decode(record.ReceiptsBatchCID)
	if err != nil {
		return nil, fmt.Errorf("parsing receipts batch CID: %w", err)
	}
	receiptsBatchCID := cidlink.Link{Cid: c}

	return &ConsolidatedRecord{
		NodeDID:          parsedDID,
		ReceiptsBatchCID: receiptsBatchCID,
		TotalBytes:       record.TotalBytes,
		ProcessedAt:      record.ProcessedAt,
	}, nil
}