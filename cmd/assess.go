package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/assess"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	assessFormat  string
	assessOutput  string
)

var assessCmd = &cobra.Command{
	Use:   "assess",
	Short: "Assess PostgreSQL to TiDB migration compatibility",
	Long: `Assess the compatibility of migrating from PostgreSQL to TiDB.

Scans all schema objects (tables, columns, indexes, views, functions,
triggers, custom types, extensions, sequences) and generates a detailed
compatibility assessment report with scores and migration suggestions.

Output formats:
  - terminal: colored table (default)
  - json: structured JSON data`,
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
		logger.InitWithOutput(logLevel, logFormat, "")

		ctx := context.Background()

		pgDB, err := sql.Open("pgx", cfg.Source.DSN())
		if err != nil {
			return fmt.Errorf("connect to PostgreSQL: %w", err)
		}
		defer pgDB.Close()

		schema := cfg.Source.Schema
		if schema == "" {
			schema = "public"
		}

		start := time.Now()
		zap.L().Info("starting compatibility assessment",
			zap.String("schema", schema))

		// Phase 1: Scan PG schema
		scanner := assess.NewScanner(pgDB, schema)
		result, err := scanner.ScanAll(ctx)
		if err != nil {
			return fmt.Errorf("scan schema: %w", err)
		}

		zap.L().Info("schema scan completed",
			zap.Int("tables", len(result.Tables)),
			zap.Int("columns", len(result.Columns)),
			zap.Int("indexes", len(result.Indexes)),
			zap.Int("views", len(result.Views)),
			zap.Int("functions", len(result.Functions)),
			zap.Int("triggers", len(result.Triggers)),
			zap.Duration("duration", time.Since(start)))

		// Phase 2: Assess compatibility
		assessor := assess.NewAssessor()
		dims := assessor.Assess(result)

		// Phase 3: Generate report
		rg := assess.NewReportGenerator(dims)
		report := rg.Report()

		zap.L().Info("assessment completed",
			zap.String("level", report.Level),
			zap.String("score", assess.FormatScore(report.Score)),
			zap.Duration("duration", time.Since(start)))

		// Output
		switch assessFormat {
		case "json":
			if assessOutput != "" {
				if err := rg.WriteJSONFile(assessOutput); err != nil {
					return fmt.Errorf("write JSON: %w", err)
				}
				fmt.Fprintf(os.Stderr, "Report written to %s\n", assessOutput)
			} else {
				rg.WriteJSON(os.Stdout)
			}
		default: // terminal
			rg.PrintTerminal(os.Stdout)
			if assessOutput != "" {
				if err := rg.WriteJSONFile(assessOutput); err != nil {
					return fmt.Errorf("write JSON: %w", err)
				}
				fmt.Fprintf(os.Stderr, "JSON report also written to %s\n", assessOutput)
			}
		}

		return nil
	},
}

func init() {
	assessCmd.Flags().StringVar(&assessFormat, "format", "terminal", "Output format: terminal, json")
	assessCmd.Flags().StringVarP(&assessOutput, "output", "o", "", "Output file path (for JSON format)")
	rootCmd.AddCommand(assessCmd)
}
