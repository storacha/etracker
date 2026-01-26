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
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/principal/signer"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/etracker/internal/config"
	"github.com/storacha/etracker/internal/consolidator"
	"github.com/storacha/etracker/internal/db/consolidated"
	"github.com/storacha/etracker/internal/db/consumer"
	"github.com/storacha/etracker/internal/db/customer"
	"github.com/storacha/etracker/internal/db/egress"
	"github.com/storacha/etracker/internal/db/spacestats"
	"github.com/storacha/etracker/internal/db/storageproviders"
	"github.com/storacha/etracker/internal/presets"
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
		"metrics-environment",
		"",
		"Environment label for metrics",
	)
	cobra.CheckErr(viper.BindPFlag("metrics_environment", startCmd.Flags().Lookup("metrics-environment")))

	cobra.CheckErr(viper.BindEnv("metrics_auth_token"))
	cobra.CheckErr(viper.BindEnv("admin_dashboard_user"))
	cobra.CheckErr(viper.BindEnv("admin_dashboard_password"))
	cobra.CheckErr(viper.BindEnv("client_egress_usd_per_tib"))
	cobra.CheckErr(viper.BindEnv("provider_egress_usd_per_tib"))

	startCmd.Flags().String(
		"egress-table-name",
		"",
		"Name of the DynamoDB table to use for egress records",
	)
	cobra.CheckErr(viper.BindPFlag("egress_table_name", startCmd.Flags().Lookup("egress-table-name")))
	// bind flag to storoku-style environment variable
	cobra.CheckErr(viper.BindEnv("egress_table_name", "EGRESS_RECORDS_TABLE_ID"))

	startCmd.Flags().String(
		"egress-unprocessed-index-name",
		"",
		"Name of the DynamoDB index to use for querying unprocessed egress records",
	)
	cobra.CheckErr(viper.BindPFlag("egress_unprocessed_index_name", startCmd.Flags().Lookup("egress-unprocessed-index-name")))
	cobra.CheckErr(viper.BindEnv("egress_unprocessed_index_name", "EGRESS_RECORDS_UNPROCESSED_INDEX_NAME"))

	startCmd.Flags().String(
		"consolidated-table-name",
		"",
		"Name of the DynamoDB table to use for consolidated records",
	)
	cobra.CheckErr(viper.BindPFlag("consolidated_table_name", startCmd.Flags().Lookup("consolidated-table-name")))
	cobra.CheckErr(viper.BindEnv("consolidated_table_name", "CONSOLIDATED_RECORDS_TABLE_ID"))

	startCmd.Flags().String(
		"consolidated-node-stats-index-name",
		"",
		"Name of the DynamoDB index to use for querying consolidated stats by node and time",
	)
	cobra.CheckErr(viper.BindPFlag("consolidated_node_stats_index_name", startCmd.Flags().Lookup("consolidated-node-stats-index-name")))
	cobra.CheckErr(viper.BindEnv("consolidated_node_stats_index_name", "CONSOLIDATED_RECORDS_NODE_STATS_INDEX_NAME"))

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

	cobra.CheckErr(viper.BindEnv("space_stats_table_name", "SPACE_STATS_TABLE_ID"))

	cobra.CheckErr(viper.BindEnv("storage_provider_table_name", "STORAGE_PROVIDER_TABLE_NAME"))
	cobra.CheckErr(viper.BindEnv("storage_provider_table_region", "STORAGE_PROVIDER_TABLE_REGION"))

	cobra.CheckErr(viper.BindEnv("customer_table_name", "CUSTOMER_TABLE_NAME"))
	cobra.CheckErr(viper.BindEnv("customer_table_region", "CUSTOMER_TABLE_REGION"))

	cobra.CheckErr(viper.BindEnv("consumer_table_name", "CONSUMER_TABLE_NAME"))
	cobra.CheckErr(viper.BindEnv("consumer_table_region", "CONSUMER_TABLE_REGION"))
	cobra.CheckErr(viper.BindEnv("consumer_consumer_index_name", "CONSUMER_CONSUMER_INDEX_NAME"))
	cobra.CheckErr(viper.BindEnv("consumer_customer_index_name", "CONSUMER_CUSTOMER_INDEX_NAME"))

	startCmd.Flags().StringSlice(
		"known-providers",
		presets.KnownProviders,
		"List of known provider DIDs (defaults to presets if not specified)",
	)
	cobra.CheckErr(viper.BindPFlag("known_providers", startCmd.Flags().Lookup("known-providers")))

	startCmd.Flags().StringSlice(
		"trusted-authorities",
		[]string{},
		"List of trusted authorities, identified by their DIDs (comma-separated)",
	)
	cobra.CheckErr(viper.BindPFlag("trusted_authorities", startCmd.Flags().Lookup("trusted-authorities")))
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

	env := cfg.MetricsEnvironment

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg.AWSConfig)

	// Create database tables
	egressTable := egress.NewDynamoEgressTable(dynamoClient, cfg.EgressTableName, cfg.EgressUnprocessedIndexName)
	consolidatedTable := consolidated.NewDynamoConsolidatedTable(dynamoClient, cfg.ConsolidatedTableName, cfg.ConsolidatedNodeStatsIndexName)
	spaceStatsTable := spacestats.NewDynamoSpaceStatsTable(dynamoClient, cfg.SpaceStatsTableName)

	storageProviderCfg := cfg.AWSConfig.Copy()
	storageProviderCfg.Region = cfg.StorageProviderTableRegion
	storageProviderTable := storageproviders.NewDynamoStorageProviderTable(dynamodb.NewFromConfig(storageProviderCfg), cfg.StorageProviderTableName)

	customerCfg := cfg.AWSConfig.Copy()
	customerCfg.Region = cfg.CustomerTableRegion
	customerTable := customer.NewDynamoCustomerTable(dynamodb.NewFromConfig(customerCfg), cfg.CustomerTableName)

	consumerCfg := cfg.AWSConfig.Copy()
	consumerCfg.Region = cfg.ConsumerTableRegion
	consumerTable := consumer.NewDynamoConsumerTable(dynamodb.NewFromConfig(consumerCfg), cfg.ConsumerTableName, cfg.ConsumerConsumerIndexName, cfg.ConsumerCustomerIndexName)

	// Create service
	svc, err := service.New(
		id,
		env,
		egressTable,
		consolidatedTable,
		storageProviderTable,
		customerTable,
		consumerTable,
		spaceStatsTable,
	)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	// Presets-based principal resolution
	presolver, err := presets.NewPresetResolver()
	if err != nil {
		return fmt.Errorf("creating principal resolver: %w", err)
	}

	// Trust attestations from trusted authorities
	var authProofs []delegation.Delegation
	for _, authority := range cfg.TrustedAuthorities {
		auth, err := did.Parse(authority)
		if err != nil {
			return fmt.Errorf("parsing trusted authority: %w", err)
		}

		attestDlg, err := delegation.Delegate(
			id,
			auth,
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability(
					ucancap.AttestAbility,
					id.DID().String(),
					ucan.NoCaveats{},
				),
			},
			delegation.WithNoExpiration(),
		)
		if err != nil {
			return err
		}

		authProofs = append(authProofs, attestDlg)
	}

	// Create and start consolidator
	interval := time.Duration(cfg.ConsolidationInterval) * time.Second
	batchSize := cfg.ConsolidationBatchSize

	cons, err := consolidator.New(
		id,
		env,
		egressTable,
		consolidatedTable,
		spaceStatsTable,
		consumerTable,
		cfg.KnownProviders,
		interval,
		batchSize,
		presolver.ResolveDIDKey,
		authProofs,
	)
	if err != nil {
		return fmt.Errorf("creating consolidator: %w", err)
	}

	// Start consolidator in a goroutine
	go cons.Start(ctx)

	// Create server
	server, err := server.New(
		id,
		svc,
		cons,
		server.WithMetricsEndpoint(cfg.MetricsAuthToken),
		server.WithAdminCreds(cfg.AdminDashboardUser, cfg.AdminDashboardPassword),
		server.WithPricing(cfg.ClientEgressUSDPerTiB, cfg.ProviderEgressUSDPerTiB),
		server.WithPrincipalResolver(presolver),
		server.WithAuthorityProofs(authProofs...),
	)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

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
