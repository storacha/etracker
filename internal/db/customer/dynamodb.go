package customer

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

var _ CustomerTable = (*DynamoCustomerTable)(nil)

type DynamoCustomerTable struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoCustomerTable(client *dynamodb.Client, tableName string) *DynamoCustomerTable {
	return &DynamoCustomerTable{client, tableName}
}

func (d *DynamoCustomerTable) List(ctx context.Context, limit int, cursor *string) (*ListResult, error) {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(d.tableName),
		ProjectionExpression: aws.String("customer"),
		Limit:                aws.Int32(int32(limit)),
	}

	// Decode cursor if provided
	if cursor != nil && *cursor != "" {
		exclusiveStartKey, err := decodeToken(*cursor)
		if err != nil {
			return nil, fmt.Errorf("decoding cursor: %w", err)
		}
		input.ExclusiveStartKey = exclusiveStartKey
	}

	result, err := d.client.Scan(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("scanning customers: %w", err)
	}

	customers := make([]did.DID, 0, len(result.Items))
	for _, item := range result.Items {
		customer, err := d.unmarshalCustomer(item)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling customer: %w", err)
		}
		customers = append(customers, *customer)
	}

	// Encode nextCursor if there are more results
	var nextCursor *string
	if result.LastEvaluatedKey != nil {
		token, err := encodeToken(result.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("encoding cursor: %w", err)
		}
		nextCursor = aws.String(token)
	}

	return &ListResult{
		Customers: customers,
		Cursor:    nextCursor,
	}, nil
}

// customerRecord is the internal struct for unmarshaling from DynamoDB
type customerRecord struct {
	Customer string `dynamodbav:"customer"`
}

func (d *DynamoCustomerTable) unmarshalCustomer(item map[string]types.AttributeValue) (*did.DID, error) {
	var record customerRecord
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("unmarshaling customer record: %w", err)
	}

	customerDID, err := did.Parse(record.Customer)
	if err != nil {
		return nil, fmt.Errorf("parsing customer DID: %w", err)
	}

	return &customerDID, nil
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

func (d *DynamoCustomerTable) Has(ctx context.Context, customerDID did.DID) (bool, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"customer": &types.AttributeValueMemberS{Value: customerDID.String()},
		},
		ProjectionExpression: aws.String("customer"),
	}

	result, err := d.client.GetItem(ctx, input)
	if err != nil {
		return false, fmt.Errorf("checking customer existence: %w", err)
	}

	return result.Item != nil, nil
}
