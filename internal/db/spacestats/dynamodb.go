package spacestats

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/storacha/go-ucanto/did"
)

var _ SpaceStatsTable = (*DynamoSpaceStatsTable)(nil)

type DynamoSpaceStatsTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoSpaceStatsTable(client *dynamodb.Client, tableName string) *DynamoSpaceStatsTable {
	return &DynamoSpaceStatsTable{client, tableName}
}

func (d *DynamoSpaceStatsTable) Record(ctx context.Context, space did.DID, egress uint64) error {
	// Format date as YYYY-MM-DD
	date := time.Now().UTC().Format("2006-01-02")

	// Use UpdateItem with ADD to atomically increment egress
	// If the item doesn't exist, it will be created with the initial value
	_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"space": &types.AttributeValueMemberS{Value: space.String()},
			"date":  &types.AttributeValueMemberS{Value: date},
		},
		UpdateExpression: aws.String("ADD egress :egress"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":egress": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", egress)},
		},
	})
	if err != nil {
		return fmt.Errorf("recording space stats: %w", err)
	}

	return nil
}

func (d *DynamoSpaceStatsTable) GetDailyStats(ctx context.Context, space did.DID, from time.Time, to time.Time) ([]DailyStats, error) {
	stats := make([]DailyStats, 0)
	var exclusiveStartKey map[string]types.AttributeValue

	// Format dates to YYYY-MM-DD for comparison
	fromDate := from.UTC().Format("2006-01-02")
	toDate := to.UTC().Format("2006-01-02")

	// Keep querying until we get all results (handle pagination)
	for {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(d.tableName),
			KeyConditionExpression: aws.String("#space = :space AND #date BETWEEN :from AND :to"),
			ExpressionAttributeNames: map[string]string{
				"#space": "space",
				"#date":  "date",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":space": &types.AttributeValueMemberS{Value: space.String()},
				":from":  &types.AttributeValueMemberS{Value: fromDate},
				":to":    &types.AttributeValueMemberS{Value: toDate},
			},
			ProjectionExpression: aws.String("#date, egress"),
		}

		// Set the pagination token if we have one
		if exclusiveStartKey != nil {
			input.ExclusiveStartKey = exclusiveStartKey
		}

		result, err := d.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("querying daily stats for space: %w", err)
		}

		// Unmarshal and accumulate results from this page
		for _, item := range result.Items {
			stat, err := d.unmarshalDailyStats(item)
			if err != nil {
				return nil, err
			}
			stats = append(stats, stat)
		}

		// Check if there are more results to fetch
		if result.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = result.LastEvaluatedKey
	}

	return stats, nil
}

// dailyStatsRecord is the internal struct for unmarshaling from DynamoDB
type dailyStatsRecord struct {
	Date   string `dynamodbav:"date"`
	Egress uint64 `dynamodbav:"egress"`
}

func (d *DynamoSpaceStatsTable) unmarshalDailyStats(item map[string]types.AttributeValue) (DailyStats, error) {
	var record dailyStatsRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return DailyStats{}, fmt.Errorf("unmarshaling daily stats record: %w", err)
	}

	date, err := time.Parse("2006-01-02", record.Date)
	if err != nil {
		return DailyStats{}, fmt.Errorf("parsing date: %w", err)
	}

	return DailyStats{
		Date:   date,
		Egress: record.Egress,
	}, nil
}
