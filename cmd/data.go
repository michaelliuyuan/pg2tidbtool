package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Migrate full data from PostgreSQL to TiDB",
	Long: `Migrate full data from PostgreSQL to TiDB using high-performance tools:
  - PostgreSQL: parallel COPY export
  - TiDB: Lightning local import`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("data migration: not implemented yet")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(dataCmd)
	dataCmd.Flags().Int("parallel", 4, "number of parallel workers")
	dataCmd.Flags().Int("batch-size", 100000, "rows per batch")
	dataCmd.Flags().StringSlice("tables", nil, "specific tables to migrate (default: all)")
	dataCmd.Flags().StringSlice("exclude-tables", nil, "tables to exclude")
	dataCmd.Flags().Bool("use-lightning", true, "use TiDB Lightning for import")
	dataCmd.Flags().String("lightning-config", "", "custom TiDB Lightning config file")
	dataCmd.Flags().String("temp-dir", "/tmp/pg2tidb", "temporary directory for data files")
}
