package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "pg2tidb",
	Short: "PostgreSQL to TiDB full data migration tool",
	Long: `pg2tidb is a CLI tool for migrating data from PostgreSQL to TiDB.

It provides:
  - Schema migration (structure, indexes, views)
  - Full data migration (high-performance parallel export/import)
  - Data validation (row count, sampling, checksum)
  - CDC incremental sync (optional module, default OFF — enable via cdc.enable
    in config, or run 'pg2tidb cdc --enable-cdc')`,
	SilenceUsage: true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "configs/config.yaml", "config file path")
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug, info, warn, error")
	rootCmd.PersistentFlags().String("log-format", "console", "log format: console, json")
}
