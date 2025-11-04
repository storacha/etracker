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
	client            *dynamodb.Client
	tableName         string
	consumerIndexName string
	customerIndexName string
}

func NewDynamoConsumerTable(client *dynamodb.Client, tableName, consumerIndexName, customerIndexName string) *DynamoConsumerTable {
	return &DynamoConsumerTable{client, tableName, consumerIndexName, customerIndexName}
}

func (d *DynamoConsumerTable) Get(ctx context.Context, consumerID string) (Consumer, error) {
	// Query the consumer index to get the item by consumer ID
	result, err := d.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		IndexName:              aws.String(d.consumerIndexName),
		KeyConditionExpression: aws.String("consumer = :consumer"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":consumer": &types.AttributeValueMemberS{Value: consumerID},
		},
	})
	if err != nil {
		return Consumer{}, fmt.Errorf("querying consumer by ID: %w", err)
	}

	if len(result.Items) == 0 {
		return Consumer{}, fmt.Errorf("consumer not found: %s", consumerID)
	}

	consumer, err := d.unmarshalConsumer(result.Items[0])
	if err != nil {
		return Consumer{}, fmt.Errorf("unmarshaling consumer: %w", err)
	}

	return *consumer, nil
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
			consumers = append(consumers, consumer.ID)
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
	Consumer     string `dynamodbav:"consumer"`
	Provider     string `dynamodbav:"provider,omitempty"`
	Subscription string `dynamodbav:"subscription,omitempty"`
}

func (d *DynamoConsumerTable) unmarshalConsumer(item map[string]types.AttributeValue) (*Consumer, error) {
	var record consumerRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling consumer record: %w", err)
	}

	consumerDID, err := did.Parse(record.Consumer)
	if err != nil {
		return nil, fmt.Errorf("parsing consumer DID: %w", err)
	}

	providerDID := did.Undef
	if record.Provider != "" {
		providerDID, err = did.Parse(record.Provider)
		if err != nil {
			return nil, fmt.Errorf("parsing provider DID: %w", err)
		}
	}

	return &Consumer{
		ID:           consumerDID,
		Provider:     providerDID,
		Subscription: record.Subscription,
	}, nil
}
