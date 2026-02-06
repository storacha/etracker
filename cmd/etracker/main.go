package main

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var log = logging.Logger("etracker")

const shortDescription = `
ETracker - Egress tracking for storage nodes
`

const longDescription = `
ETracker records content served by storage nodes so that they can be paid for the corresponding egress.
`

var (
	cfgFile string

	logLevel string

	rootCmd = &cobra.Command{
		Use:   "etracker",
		Short: shortDescription,
		Long:  longDescription,
	}
)

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "logging level")

	rootCmd.PersistentFlags().String("key-file", "", "Path to a PEM file containing ed25519 private key")
	cobra.CheckErr(rootCmd.MarkPersistentFlagFilename("key-file", "pem"))
	cobra.CheckErr(viper.BindPFlag("key_file", rootCmd.PersistentFlags().Lookup("key-file")))

	// register all commands and their subcommands
	rootCmd.AddCommand(startCmd)
}

func initConfig() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("ETRACKER")

	if logLevel != "" {
		ll, err := logging.LevelFromString(logLevel)
		cobra.CheckErr(err)
		logging.SetAllLoggers(ll)
	}

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		cobra.CheckErr(viper.ReadInConfig())
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
