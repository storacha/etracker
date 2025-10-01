package egress

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
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

type egressRecord struct {
	// Partition key: "DATE#SHARD" (e.g., "2025-08-18#0")
	// Where SHARD is a number 0-9 to distribute writes
	PK string `dynamodbav:"PK"`

	// Sort key: "RECEIVED_AT#NODE_ID#UNIQUE_ID"
	// This allows sorting by time within each date partition
	SK string `dynamodbav:"SK"`

	Cause      string `dynamodbav:"cause"`
	NodeID     string `dynamodbav:"nodeID"`
	Receipts   string `dynamodbav:"receipts"`
	Endpoint   string `dynamodbav:"endpoint"`
	ReceivedAt string `dynamodbav:"receivedAt"`
}

func newRecord(cause ucan.Link, nodeID did.DID, receipts ucan.Link, endpoint *url.URL) egressRecord {
	// TODO: review keys to improve performance and access patterns
	receivedAt := time.Now().UTC()
	dateStr := receivedAt.Format("2006-01-02")
	shard := rand.Intn(10)
	pk := fmt.Sprintf("%s#%d", dateStr, shard)
	sk := fmt.Sprintf("%s#%s#%s", dateStr, nodeID, uuid.New())

	return egressRecord{
		PK:         pk,
		SK:         sk,
		Cause:      cause.String(),
		NodeID:     nodeID.String(),
		Receipts:   receipts.String(),
		Endpoint:   endpoint.String(),
		ReceivedAt: receivedAt.Format(time.RFC3339),
	}
}

func (d *DynamoEgressTable) Record(ctx context.Context, cause ucan.Link, nodeID did.DID, receipts ucan.Link, endpoint *url.URL) error {
	item, err := attributevalue.MarshalMap(newRecord(cause, nodeID, receipts, endpoint))
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
