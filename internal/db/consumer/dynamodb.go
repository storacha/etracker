package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/storacha/go-ucanto/did"
)

var _ ConsumerTable = (*DynamoConsumerTable)(nil)

type DynamoConsumerTable struct {
	client              *dynamodb.Client
	tableName           string
	customerIndexName   string
}

func NewDynamoConsumerTable(client *dynamodb.Client, tableName string, customerIndexName string) *DynamoConsumerTable {
	return &DynamoConsumerTable{client, tableName, customerIndexName}
}

func (d *DynamoConsumerTable) ListByCustomer(ctx context.Context, customerID did.DID) ([]did.DID, error) {
	consumers := make([]did.DID, 0)
	var exclusiveStartKey map[string]types.AttributeValue

	// Keep querying until we get all results (handle pagination)
	for {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(d.tableName),
			IndexName:              aws.String(d.customerIndexName),
			KeyConditionExpression: aws.String("customer = :customer"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":customer": &types.AttributeValueMemberS{Value: customerID.String()},
			},
			ProjectionExpression: aws.String("consumer"),
		}

		// Set the pagination token if we have one
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		result, err := d.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("querying consumers by customer: %w", err)
		}

		// Unmarshal and accumulate results from this page
		for _, item := range result.Items {
			consumer, err := d.unmarshalConsumer(item)
			if err != nil {
				return nil, err
			}
			consumers = append(consumers, consumer)
		}

		// Check if there are more results to fetch
		if result.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = result.LastEvaluatedKey
	}

	return consumers, nil
}

// consumerRecord is the internal struct for unmarshaling from DynamoDB
type consumerRecord struct {
	Consumer string `dynamodbav:"consumer"`
}

func (d *DynamoConsumerTable) unmarshalConsumer(item map[string]types.AttributeValue) (did.DID, error) {
	var record consumerRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return did.DID{}, fmt.Errorf("unmarshaling consumer record: %w", err)
	}

	consumerDID, err := did.Parse(record.Consumer)
	if err != nil {
		return did.DID{}, fmt.Errorf("parsing consumer DID: %w", err)
	}

	return consumerDID, nil
}
