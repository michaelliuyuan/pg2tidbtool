package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/pg2tidb/pg2tidb-migrator/internal/cdc"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var cdcCmd = &cobra.Command{
	Use:   "cdc",
	Short: "Start CDC incremental sync (PostgreSQL → TiDB)",
	Long: `Start change data capture (CDC) incremental sync from PostgreSQL to TiDB.

Uses PostgreSQL logical replication (pgoutput plugin) to stream changes
in real-time and apply them to the TiDB target.

Prerequisites:
  - PostgreSQL wal_level = logical
  - PostgreSQL max_replication_slots >= 1
  - Target TiDB must already have the base schema migrated`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		// CDC is an OPTIONAL module (cdc.enable defaults to false). Resolve the
		// effective enable state with priority flag > env > yaml, then gate the
		// subcommand. An explicit `pg2tidb cdc` invocation is treated as opt-in
		// intent: we do not hard-refuse, but print a clear enable hint and exit
		// unless the user opts in via --enable-cdc or PG2TIDB_CDC_FORCE=1.
		enabled := resolveCDCEnableFromCmd(cfg.CDC.Enable, cmd)
		forced := os.Getenv("PG2TIDB_CDC_FORCE") == "1"
		if !enabled && !forced {
			fmt.Fprintln(os.Stderr, "CDC 模块当前未启用（cdc.enable=false）。")
			fmt.Fprintln(os.Stderr, "  本次运行: pg2tidb cdc --enable-cdc  （或设环境变量 PG2TIDB_CDC_FORCE=1）")
			fmt.Fprintln(os.Stderr, "  持久开启: 在 config.yaml 设置 cdc.enable: true")
			return fmt.Errorf("cdc module disabled (use --enable-cdc, PG2TIDB_CDC_FORCE=1, or set cdc.enable: true)")
		}
		if forced && !enabled {
			fmt.Fprintln(os.Stderr, "note: CDC forced on via PG2TIDB_CDC_FORCE=1 (cdc.enable=false in config).")
		}

		// Build CDC source config from main config
		srcCfg := cdc.DefaultSourceConfig()
		srcCfg.Host = cfg.Source.Host
		srcCfg.Port = cfg.Source.Port
		srcCfg.User = cfg.Source.User
		srcCfg.Password = cfg.Source.Password
		srcCfg.Database = cfg.Source.Database
		srcCfg.SSLMode = cfg.Source.SSLMode
		srcCfg.Tables = cfg.CDC.Tables
		srcCfg.ExcludeTables = cfg.CDC.ExcludeTables

		// CDC params default to cfg.CDC.* (see CDCConfig); explicit flags override.
		srcCfg.SlotName = cfg.CDC.SlotName
		if cmd.Flags().Changed("slot") {
			srcCfg.SlotName, _ = cmd.Flags().GetString("slot")
		}
		srcCfg.Publication = cfg.CDC.Publication
		if cmd.Flags().Changed("publication") {
			srcCfg.Publication, _ = cmd.Flags().GetString("publication")
		}
		cpFile := cfg.CDC.CheckpointFile
		if cmd.Flags().Changed("checkpoint-file") {
			cpFile, _ = cmd.Flags().GetString("checkpoint-file")
		}
		dataDir, _ := cmd.Flags().GetString("data-dir")
		if dataDir == "" {
			dataDir = ".pg2tidb"
		}
		statusFile, _ := cmd.Flags().GetString("status-file")
		if statusFile == "" {
			// CDC→Web status channel (#t48 B). Default is <data-dir>/cdc/status.json
			// (shared with the web server's --data). Log the resolved absolute path so
			// a CDC/web cwd mismatch is VISIBLE, not a silent not_running.
			statusFile = filepath.Join(dataDir, "cdc", "status.json")
		}
		if abs, err := filepath.Abs(statusFile); err == nil {
			fmt.Fprintf(os.Stderr, "cdc status file (web must read this path): %s\n", abs)
		}

		// Build batch config (defaults from cfg.CDC; explicit flags override)
		batchCfg := cdc.DefaultBatchConfig()
		batchCfg.BatchSize = cfg.CDC.BatchSize
		batchCfg.Parallel = cfg.CDC.Parallel
		batchCfg.ConflictStrategy = cdc.ConflictStrategy(cfg.CDC.ConflictStrategy)
		if cmd.Flags().Changed("batch-size") {
			if v, _ := cmd.Flags().GetInt("batch-size"); v > 0 {
				batchCfg.BatchSize = v
			}
		}
		if cmd.Flags().Changed("parallel") {
			if v, _ := cmd.Flags().GetInt("parallel"); v > 0 {
				batchCfg.Parallel = v
			}
		}
		if cmd.Flags().Changed("conflict-strategy") {
			if v, _ := cmd.Flags().GetString("conflict-strategy"); v != "" {
				batchCfg.ConflictStrategy = cdc.ConflictStrategy(v)
			}
		}

		// Build table filter
		includeTables, _ := cmd.Flags().GetStringSlice("include-table")
		excludeTables, _ := cmd.Flags().GetStringSlice("exclude-table")
		includeSchemas, _ := cmd.Flags().GetStringSlice("include-schema")
		excludeSchemas, _ := cmd.Flags().GetStringSlice("exclude-schema")

		tblFilter := cdc.NewTableFilter().
			WithWhitelist(includeTables).
			WithBlacklist(excludeTables).
			WithSchemas(includeSchemas, excludeSchemas)

		// Build TiDB target DSN
		targetDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s&readTimeout=300s&writeTimeout=300s",
			cfg.Target.User, cfg.Target.Password, cfg.Target.Host, cfg.Target.Port, cfg.Target.Database)

		// Setup logging
		logLevel, _ := cmd.Flags().GetString("log-level")
		logFormat, _ := cmd.Flags().GetString("log-format")
		logOutput, _ := cmd.Flags().GetString("log-output")
		if logLevel == "" {
			logLevel = cfg.Logging.Level
		}
		if logFormat == "" {
			logFormat = cfg.Logging.Format
		}
		logger.InitWithOutput(logLevel, logFormat, logOutput)
		defer logger.Sync()
		log := zap.L()

		runnerCfg := cdc.RunnerConfig{
			Source:            srcCfg,
			Batch:             batchCfg,
			Transformer:       cdc.DefaultTransformerConfig(),
			Filter:            tblFilter,
			TargetDSN:         targetDSN,
			CheckpointFile:    cpFile,
			StatusFile:        statusFile,
			EnableDDLTracking: cfg.CDC.SyncDDL,
		}

		runner, err := cdc.NewRunner(runnerCfg)
		if err != nil {
			return fmt.Errorf("create cdc runner: %w", err)
		}
		runner.SetLogger(log)

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		// Handle OS signals
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Info("received interrupt signal")
			cancel()
		}()

		log.Info("starting cdc incremental sync",
			zap.String("source", fmt.Sprintf("%s:%d/%s", srcCfg.Host, srcCfg.Port, srcCfg.Database)),
			zap.String("target", fmt.Sprintf("%s:%d/%s", cfg.Target.Host, cfg.Target.Port, cfg.Target.Database)),
			zap.String("slot", srcCfg.SlotName),
			zap.String("publication", srcCfg.Publication),
		)

		if err := runner.Run(ctx); err != nil && err != context.Canceled {
			return fmt.Errorf("cdc run: %w", err)
		}

		// Print final stats
		stats := runner.Stats()
		fmt.Fprintf(os.Stderr, "\n=== CDC Final Stats ===\n")
		for k, v := range stats {
			fmt.Fprintf(os.Stderr, "  %s: %v\n", k, v)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(cdcCmd)

	// CDC module enable/disable (optional module, default off — see cdc.enable).
	cdcCmd.Flags().Bool("enable-cdc", false, "enable the CDC module for this run (opt-in when cdc.enable=false)")
	cdcCmd.Flags().Bool("disable-cdc", false, "disable the CDC module for this run (overrides cdc.enable=true)")

	// CDC-specific flags
	cdcCmd.Flags().String("slot", "pg2tidb_cdc", "replication slot name")
	cdcCmd.Flags().String("publication", "pg2tidb_pub", "publication name")
	cdcCmd.Flags().String("checkpoint-file", ".cdc_checkpoint.json", "LSN checkpoint file path")
	cdcCmd.Flags().String("data-dir", ".pg2tidb", "data directory shared with the web UI (CDC status file lives under <data-dir>/cdc/)")
	cdcCmd.Flags().String("status-file", "", "CDC→Web status JSON path (defaults to <data-dir>/cdc/status.json; the web server must read the same path — #t48 B)")
	cdcCmd.Flags().Int("batch-size", 1000, "max events per apply batch")
	cdcCmd.Flags().Int("parallel", 1, "parallel apply workers (default 1=serial, correctness-first; >1 routes per-table but does NOT guarantee cross-table FK order / multi-table txn atomicity — see #t48 Bug#8)")
	cdcCmd.Flags().String("conflict-strategy", "replace", "conflict resolution: replace, insert_ignore, upsert, skip")

	// Table filter flags
	cdcCmd.Flags().StringSlice("include-table", nil, "whitelist tables (schema.table, can use *)")
	cdcCmd.Flags().StringSlice("exclude-table", nil, "blacklist tables (schema.table, can use *)")
	cdcCmd.Flags().StringSlice("include-schema", nil, "whitelist schemas")
	cdcCmd.Flags().StringSlice("exclude-schema", nil, "blacklist schemas")

	// Logging flags
	cdcCmd.Flags().String("log-level", "", "log level: debug, info, warn, error")
	cdcCmd.Flags().String("log-format", "", "log format: console, json")
	cdcCmd.Flags().String("log-output", "", "log output file path")
}
