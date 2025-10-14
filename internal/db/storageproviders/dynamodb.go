package storageproviders

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/storacha/go-ucanto/did"
)

var _ StorageProviderTable = (*DynamoStorageProviderTable)(nil)

type DynamoStorageProviderTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoStorageProviderTable(client *dynamodb.Client, tableName string) *DynamoStorageProviderTable {
	return &DynamoStorageProviderTable{client, tableName}
}

func (d *DynamoStorageProviderTable) Get(ctx context.Context, provider did.DID) (*StorageProviderRecord, error) {
	result, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"provider": &types.AttributeValueMemberS{Value: provider.String()},
		},
		ProjectionExpression: aws.String("provider, operatorEmail, endpoint"),
	})
	if err != nil {
		return nil, fmt.Errorf("getting storage provider: %w", err)
	}

	if result.Item == nil {
		return nil, ErrNotFound
	}

	return d.unmarshalRecord(result.Item)
}

func (d *DynamoStorageProviderTable) GetAll(ctx context.Context) ([]StorageProviderRecord, error) {
	result, err := d.client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String(d.tableName),
		ProjectionExpression: aws.String("provider, operatorEmail, endpoint"),
	})
	if err != nil {
		return nil, fmt.Errorf("scanning storage providers: %w", err)
	}

	records := make([]StorageProviderRecord, 0, len(result.Items))
	for _, item := range result.Items {
		record, err := d.unmarshalRecord(item)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling storage provider record: %w", err)
		}
		records = append(records, *record)
	}

	return records, nil
}

// storageProviderRecord is the internal struct for unmarshaling from DynamoDB
// We only unmarshal the fields we need
type storageProviderRecord struct {
	Provider      string `dynamodbav:"provider"`
	OperatorEmail string `dynamodbav:"operatorEmail"`
	Endpoint      string `dynamodbav:"endpoint"`
}

func (d *DynamoStorageProviderTable) unmarshalRecord(item map[string]types.AttributeValue) (*StorageProviderRecord, error) {
	var record storageProviderRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling storage provider record: %w", err)
	}

	provider, err := did.Parse(record.Provider)
	if err != nil {
		return nil, fmt.Errorf("parsing provider DID: %w", err)
	}

	return &StorageProviderRecord{
		Provider:      provider,
		OperatorEmail: record.OperatorEmail,
		Endpoint:      record.Endpoint,
	}, nil
}
