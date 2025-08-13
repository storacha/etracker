package config

import "github.com/spf13/viper"

type Config struct {
	PrivateKey string `mapstructure:"private_key" validate:"required"`
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
