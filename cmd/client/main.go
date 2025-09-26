package main

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
)

var log = logging.Logger("client")

const shortDescription = `
ETracker Client - Send egress tracking invocations to a tracker service
`

const longDescription = `
The etracker client builds and sends egress tracking invocations to a tracker service.

By default, it will use the egress tracker service in the staging warm network.
`

var (
	logLevel string

	rootCmd = &cobra.Command{
		Use:   "etclient",
		Short: shortDescription,
		Long:  longDescription,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "debug", "logging level")

	// register all commands and their subcommands
	rootCmd.AddCommand(trackCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
