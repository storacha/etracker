package storageproviders

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

func (d *DynamoStorageProviderTable) GetAll(ctx context.Context, limit int, startToken *string) (*GetAllResult, error) {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(d.tableName),
		ProjectionExpression: aws.String("provider, operatorEmail, endpoint"),
		Limit:                aws.Int32(int32(limit)),
	}

	// Decode startToken if provided
	if startToken != nil && *startToken != "" {
		exclusiveStartKey, err := decodeToken(*startToken)
		if err != nil {
			return nil, fmt.Errorf("decoding start token: %w", err)
		}
		input.ExclusiveStartKey = exclusiveStartKey
	}

	result, err := d.client.Scan(ctx, input)
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

	// Encode nextToken if there are more results
	var nextToken *string
	if result.LastEvaluatedKey != nil {
		token, err := encodeToken(result.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("encoding next token: %w", err)
		}
		nextToken = aws.String(token)
	}

	return &GetAllResult{
		Records:   records,
		NextToken: nextToken,
	}, nil
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

// encodeToken encodes a DynamoDB LastEvaluatedKey into a base64 string token
func encodeToken(key map[string]types.AttributeValue) (string, error) {
	if key == nil {
		return "", nil
	}

	jsonBytes, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("marshaling key to JSON: %w", err)
	}

	return base64.URLEncoding.EncodeToString(jsonBytes), nil
}

// decodeToken decodes a base64 string token into a DynamoDB ExclusiveStartKey
func decodeToken(token string) (map[string]types.AttributeValue, error) {
	if token == "" {
		return nil, nil
	}

	jsonBytes, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 token: %w", err)
	}

	var key map[string]types.AttributeValue
	if err := json.Unmarshal(jsonBytes, &key); err != nil {
		return nil, fmt.Errorf("unmarshaling JSON to key: %w", err)
	}

	return key, nil
}
