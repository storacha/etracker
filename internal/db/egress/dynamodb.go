package egress

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
)

type DynamoEgressTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoEgressTable(client *dynamodb.Client, tableName string) *DynamoEgressTable {
	return &DynamoEgressTable{client, tableName}
}

// TODO: add keys to improve performance and access patterns
type egressRecord struct {
	NodeID     string    `dynamodbav:"nodeID"`
	Receipts   []string  `dynamodbav:"receipts"`
	Endpoint   string    `dynamodbav:"endpoint"`
	ReceivedAt time.Time `dynamodbav:"receivedAt"`
}

func (d *DynamoEgressTable) Record(ctx context.Context, nodeID did.DID, receipts []ucan.Link, endpoint *url.URL) error {
	rcptsStrs := make([]string, len(receipts))
	for i, rcpt := range receipts {
		rcptsStrs[i] = rcpt.String()
	}

	item, err := attributevalue.MarshalMap(egressRecord{
		NodeID:     nodeID.String(),
		Receipts:   rcptsStrs,
		Endpoint:   endpoint.String(),
		ReceivedAt: time.Now().UTC(),
	})
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
