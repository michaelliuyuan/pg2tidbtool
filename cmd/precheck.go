package cmd

import (
	"fmt"

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
		fmt.Println("pre-check: not implemented yet")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(precheckCmd)
	precheckCmd.Flags().String("report", "precheck-report.json", "output report file path")
}
