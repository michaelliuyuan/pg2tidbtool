package cmd

import (
	"fmt"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/data"
	"github.com/spf13/cobra"
)

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Migrate full data from PostgreSQL to TiDB",
	Long: `Migrate full data from PostgreSQL to TiDB using high-performance tools:
  - PostgreSQL: parallel COPY export
  - TiDB: Lightning local import`,
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
		logger.InitWithOutput(logLevel, logFormat, cfg.Logging.Output)
		defer logger.Sync()

		parallel, _ := cmd.Flags().GetInt("parallel")
		batchSize, _ := cmd.Flags().GetInt("batch-size")
		tables, _ := cmd.Flags().GetStringSlice("tables")
		excludeTables, _ := cmd.Flags().GetStringSlice("exclude-tables")
		useLightning, _ := cmd.Flags().GetBool("use-lightning")
		lightningCfg, _ := cmd.Flags().GetString("lightning-config")
		tempDir, _ := cmd.Flags().GetString("temp-dir")

		m := data.NewMigrator(*cfg)
		result, err := m.Run(cmd.Context(), common.DataOpts{
			Parallel:      parallel,
			BatchSize:     batchSize,
			Tables:        tables,
			ExcludeTables: excludeTables,
			UseLightning:  useLightning,
			LightningCfg:  lightningCfg,
			TempDir:       tempDir,
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStderr(), "Data migration completed: %d rows, %d tables, %s\n",
			result.TotalRows, result.TotalTables, result.Duration)
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
