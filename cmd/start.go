package main

import (
	"fmt"

	"github.com/spf13/cobra"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"

	"github.com/storacha/payme/internal/config"
	"github.com/storacha/payme/internal/ucanserver"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start PayMe server",
	Args:  cobra.NoArgs,
	RunE:  startService,
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

	ucanServer, err := ucanserver.New(id)
	if err != nil {
		return fmt.Errorf("creating UCAN server: %w", err)
	}

	return ucanServer.ListenAndServe(":8080")
}
