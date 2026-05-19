package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Migrate PostgreSQL schema to TiDB",
	Long: `Migrate database schema objects from PostgreSQL to TiDB, including:
  - Tables (with type mapping)
  - Indexes
  - Views
  - Sequences
  - Constraints`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("schema migration: not implemented yet")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)
	schemaCmd.Flags().Bool("dry-run", false, "only generate DDL without executing")
	schemaCmd.Flags().String("output", "", "output DDL to file instead of executing")
	schemaCmd.Flags().StringSlice("schemas", nil, "specific schemas to migrate (default: all)")
	schemaCmd.Flags().StringSlice("exclude-tables", nil, "tables to exclude from migration")
}
