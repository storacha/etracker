package config

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/viper"
)

type Config struct {
	Port            int        `mapstructure:"port" validate:"required,min=1,max=65535"`
	PrivateKey      string     `mapstructure:"private_key" validate:"required"`
	DID             string     `mapstructure:"did" validate:"startswith=did:web:"`
	AWSConfig       aws.Config `mapstructure:"aws_config"`
	EgressTableName string     `mapstructure:"egress_table_name" validate:"required"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}
