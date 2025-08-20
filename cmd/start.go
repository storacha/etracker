package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/principal/signer"

	"github.com/storacha/etracker/internal/config"
	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/server"
	"github.com/storacha/etracker/internal/service"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start ETracker",
	Args:  cobra.NoArgs,
	RunE:  startService,
}

func init() {
	startCmd.Flags().Int(
		"port",
		8080,
		"Port to listen on",
	)
	cobra.CheckErr(viper.BindPFlag("port", startCmd.Flags().Lookup("port")))

	startCmd.Flags().String(
		"private-key",
		"",
		"Private key the service will use as its identity",
	)
	cobra.CheckErr(viper.BindPFlag("private_key", startCmd.Flags().Lookup("private-key")))

	startCmd.Flags().String(
		"did",
		"",
		"did:web the service will use as its identity",
	)
	cobra.CheckErr(viper.BindPFlag("did", startCmd.Flags().Lookup("did")))

	startCmd.Flags().String(
		"egress-table-name",
		"",
		"Name of the DynamoDB table to use for egress records",
	)
	cobra.CheckErr(viper.BindPFlag("egress_table_name", startCmd.Flags().Lookup("egress-table-name")))
}

func startService(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	id, err := ed25519.Parse(cfg.PrivateKey)
	if err != nil {
		return fmt.Errorf("parsing private key: %w", err)
	}

	if cfg.DID != "" {
		d, err := did.Parse(cfg.DID)
		if err != nil {
			return fmt.Errorf("parsing DID: %w", err)
		}
		id, err = signer.Wrap(id, d)
		if err != nil {
			return fmt.Errorf("wrapping server DID: %w", err)
		}
	}

	svc, err := service.New(id, egress.NewDynamoEgressTable(dynamodb.NewFromConfig(cfg.AWSConfig), cfg.EgressTableName))
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	server, err := server.New(id, svc)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	return server.ListenAndServe(fmt.Sprintf(":%d", cfg.Port))
}
