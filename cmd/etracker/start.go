package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/principal/signer"

	"github.com/storacha/etracker/internal/config"
	"github.com/storacha/etracker/internal/consolidator"
	"github.com/storacha/etracker/internal/db/consolidated"
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

	cobra.CheckErr(viper.BindEnv("metrics_auth_token"))

	startCmd.Flags().String(
		"egress-table-name",
		"",
		"Name of the DynamoDB table to use for egress records",
	)
	cobra.CheckErr(viper.BindPFlag("egress_table_name", startCmd.Flags().Lookup("egress-table-name")))
	// bind flag to storoku-style environment variable
	cobra.CheckErr(viper.BindEnv("egress_table_name", "EGRESS_RECORDS_TABLE_ID"))

	startCmd.Flags().String(
		"consolidated-table-name",
		"",
		"Name of the DynamoDB table to use for consolidated records",
	)
	cobra.CheckErr(viper.BindPFlag("consolidated_table_name", startCmd.Flags().Lookup("consolidated-table-name")))
	cobra.CheckErr(viper.BindEnv("consolidated_table_name", "CONSOLIDATED_RECORDS_TABLE_ID"))

	startCmd.Flags().Int(
		"consolidation-interval",
		12*60*60,
		"Interval in seconds between consolidation runs",
	)
	cobra.CheckErr(viper.BindPFlag("consolidation_interval", startCmd.Flags().Lookup("consolidation-interval")))

	startCmd.Flags().Int(
		"consolidation-batch-size",
		100,
		"Number of records to process in each consolidation batch",
	)
	cobra.CheckErr(viper.BindPFlag("consolidation_batch_size", startCmd.Flags().Lookup("consolidation-batch-size")))
}

func startService(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	cfg, err := config.Load(ctx)
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

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg.AWSConfig)

	// Create database tables
	egressTable := egress.NewDynamoEgressTable(dynamoClient, cfg.EgressTableName)
	consolidatedTable := consolidated.NewDynamoConsolidatedTable(dynamoClient, cfg.ConsolidatedTableName)

	// Create service
	svc, err := service.New(id, egressTable)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	// Create server
	server, err := server.New(id, svc, server.WithMetricsEndpoint(cfg.MetricsAuthToken))
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	// Create and start consolidator
	interval := time.Duration(cfg.ConsolidationInterval) * time.Second
	batchSize := cfg.ConsolidationBatchSize

	cons, err := consolidator.New(id, egressTable, consolidatedTable, interval, batchSize)
	if err != nil {
		return fmt.Errorf("creating consolidator: %w", err)
	}

	// Start consolidator in a goroutine
	go cons.Start(ctx)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		log.Infof("Starting server on port %d", cfg.Port)
		errCh <- server.ListenAndServe(fmt.Sprintf(":%d", cfg.Port))
	}()

	select {
	case err := <-errCh:
		log.Errorf("Server error: %v", err)
		cons.Stop()
		return err
	case sig := <-sigCh:
		log.Infof("Received signal %v, shutting down gracefully", sig)
		cons.Stop()
		cancel()
		return nil
	}
}
