package cmd

import (
	"fmt"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/validator"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate data consistency between PostgreSQL and TiDB",
	Long: `Validate data consistency between source and target databases:
  - L1: Row count check
  - L2: Sampling data comparison
  - L3: Full checksum verification`,
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

		level, _ := cmd.Flags().GetString("level")
		sampleRatio, _ := cmd.Flags().GetFloat64("sample-ratio")
		tables, _ := cmd.Flags().GetStringSlice("tables")
		reportFile, _ := cmd.Flags().GetString("report")

		v := validator.NewValidator(*cfg)
		rpt, err := v.Run(cmd.Context(), common.ValidateOpts{
			Level:       level,
			SampleRatio: sampleRatio,
			Tables:      tables,
			ReportFile:  reportFile,
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStderr(), "Validation completed: %s (%d pass, %d fail, %d warn)\n",
			rpt.Status, rpt.Stats.PassTables, rpt.Stats.FailTables, rpt.Stats.WarnTables)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().String("level", "L2", "validation level: L1 (row count), L2 (sampling), L3 (checksum)")
	validateCmd.Flags().Float64("sample-ratio", 0.01, "sample ratio for L2 validation (0.0-1.0)")
	validateCmd.Flags().StringSlice("tables", nil, "specific tables to validate (default: all)")
	validateCmd.Flags().String("report", "validation-report.json", "output report file path")
}
