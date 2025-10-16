package config

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/viper"
)

type Config struct {
	Port                           int        `mapstructure:"port" validate:"required,min=1,max=65535"`
	PrivateKey                     string     `mapstructure:"private_key" validate:"required"`
	DID                            string     `mapstructure:"did" validate:"startswith=did:web:"`
	MetricsAuthToken               string     `mapstructure:"metrics_auth_token"`
	AdminDashboardUser             string     `mapstructure:"admin_dashboard_user"`
	AdminDashboardPassword         string     `mapstructure:"admin_dashboard_password"`
	AWSConfig                      aws.Config `mapstructure:"aws_config"`
	EgressTableName                string     `mapstructure:"egress_table_name" validate:"required"`
	EgressUnprocessedIndexName     string     `mapstructure:"egress_unprocessed_index_name" validate:"required"`
	ConsolidatedTableName          string     `mapstructure:"consolidated_table_name" validate:"required"`
	ConsolidatedNodeStatsIndexName string     `mapstructure:"consolidated_node_stats_index_name" validate:"required"`
	ConsolidationInterval          int        `mapstructure:"consolidation_interval" validate:"min=300"`
	ConsolidationBatchSize         int        `mapstructure:"consolidation_batch_size" validate:"min=1"`
	StorageProviderTableName       string     `mapstructure:"storage_provider_table_name" validate:"required"`
	StorageProviderTableRegion     string     `mapstructure:"storage_provider_table_region" validate:"required"`
	CustomerTableName              string     `mapstructure:"customer_table_name" validate:"required"`
	CustomerTableRegion            string     `mapstructure:"customer_table_region" validate:"required"`
	ConsumerTableName              string     `mapstructure:"consumer_table_name" validate:"required"`
	ConsumerTableRegion            string     `mapstructure:"consumer_table_region" validate:"required"`
	ConsumerCustomerIndexName      string     `mapstructure:"consumer_customer_index_name" validate:"required"`
}

func Load(ctx context.Context) (*Config, error) {
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS default config: %w", err)
	}

	cfg.AWSConfig = awsConfig

	return &cfg, nil
}
