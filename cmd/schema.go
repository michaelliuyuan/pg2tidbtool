package cmd

import (
	"fmt"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/schema"
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
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}

		logLevel, _ := cmd.Flags().GetString("log-level")
		logFormat, _ := cmd.Flags().GetString("log-format")
		logger.Init(logLevel, logFormat, cfg.Logging.Output)
		defer logger.Sync()

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		outputFile, _ := cmd.Flags().GetString("output")
		excludeTables, _ := cmd.Flags().GetStringSlice("exclude-tables")

		m := schema.NewMigrator(*cfg)
		return m.Run(cmd.Context(), common.SchemaOpts{
			DryRun:        dryRun,
			OutputFile:    outputFile,
			ExcludeTables: excludeTables,
		})
	},
}

func init() {
	rootCmd.AddCommand(schemaCmd)
	schemaCmd.Flags().Bool("dry-run", false, "only generate DDL without executing")
	schemaCmd.Flags().String("output", "", "output DDL to file instead of executing")
	schemaCmd.Flags().StringSlice("schemas", nil, "specific schemas to migrate (default: all)")
	schemaCmd.Flags().StringSlice("exclude-tables", nil, "tables to exclude from migration")
}
