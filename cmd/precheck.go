package cmd

import (
	"fmt"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/pg2tidb/pg2tidb-migrator/internal/precheck"
	"github.com/spf13/cobra"
)

var precheckCmd = &cobra.Command{
	Use:   "precheck",
	Short: "Pre-check compatibility between PostgreSQL and TiDB",
	Long: `Run pre-migration checks:
  - Database connectivity
  - Disk space estimation
  - Incompatible object scanning
  - Compatibility report generation`,
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

		reportFile, _ := cmd.Flags().GetString("report")

		c := precheck.NewChecker(*cfg)
		rpt, err := c.Run(cmd.Context(), common.PrecheckOpts{
			ReportFile: reportFile,
		})
		if err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStderr(), "Pre-check completed: %s (%d pass, %d warn, %d fail)\n",
			rpt.Status, rpt.Stats.PassTables, rpt.Stats.WarnTables, rpt.Stats.FailTables)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(precheckCmd)
	precheckCmd.Flags().String("report", "precheck-report.json", "output report file path")
}
