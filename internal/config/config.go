package config

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/viper"
)

type Config struct {
	Port            int        `mapstructure:"port" validate:"required,min=1,max=65535"`
	PrivateKey      string     `mapstructure:"private_key" validate:"required"`
	DID             string     `mapstructure:"did" validate:"startswith=did:web:"`
	AWSConfig       aws.Config `mapstructure:"aws_config"`
	EgressTableName string     `mapstructure:"egress_table_name" validate:"required"`
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
