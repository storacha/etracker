package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var trackCmd = &cobra.Command{
	Use:   "track",
	Short: "invoke `space/egress/track` on an Egress Tracking Service",
	Args:  cobra.NoArgs,
	RunE:  trackEgress,
}

func init() {
	trackCmd.Flags().String(
		"etracker-did",
		"",
		"did:web of the Egress Tracking Service",
	)
	cobra.CheckErr(viper.BindPFlag("etracker_did", trackCmd.Flags().Lookup("etracker-did")))

	trackCmd.Flags().String(
		"etracker-delegation",
		"",
		"a base64-encoded, CAR-formatted delegation from the Egress Tracking Service allowing this client to invoke `space/egress/track`",
	)
	cobra.CheckErr(viper.BindPFlag("etracker_delegation", trackCmd.Flags().Lookup("etracker-delegation")))
}

func trackEgress(cmd *cobra.Command, args []string) error {
	// Parse flags

	// Build invocation

	// Execute invocation

	// Print receipt

	return nil
}
