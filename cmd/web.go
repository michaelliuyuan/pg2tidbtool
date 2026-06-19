package cmd

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	"github.com/pg2tidb/pg2tidb-migrator/internal/store"
	"github.com/pg2tidb/pg2tidb-migrator/internal/webapi"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	webPort       int
	webHost       string
	webData       string
	cdcStatusFile string
	cdcStaleSec   int
)

var webCmd = &cobra.Command{
	Use:   "web",
	Short: "Start web UI server for migration management",
	Long: `Start a web-based management interface for configuring and running
PostgreSQL to TiDB migrations. Provides a visual wizard, real-time progress
monitoring, and migration history management.

Default URL: http://localhost:8080`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataDir := webData
		if dataDir == "" {
			dataDir = ".pg2tidb"
		}

		s, err := store.NewStore(dataDir)
		if err != nil {
			return fmt.Errorf("init store: %w", err)
		}
		defer s.Close()

		// Load config to read the CDC module switch (cdc.enable) and inject it
		// into the web server (D3 #t53).
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		// CDC status file (READ channel) — resolved before NewServer so the
		// supervisor (#t55) can spawn the CDC child with the matching
		// --status-file, and so a CDC/web cwd mismatch is VISIBLE.
		statusFile := cdcStatusFile
		if statusFile == "" {
			statusFile = filepath.Join(dataDir, "cdc", "status.json")
		}
		if abs, err := filepath.Abs(statusFile); err == nil {
			fmt.Fprintf(os.Stderr, "cdc status file (dashboard reads): %s\n", abs)
		}

		// CDC control supervisor (#t55): spawn / supervise / restart / stop the
		// CDC child on behalf of the Web UI (CONTROL channel).
		bin, _ := os.Executable()
		cdcSup := webapi.NewCDCSupervisor(cfg.CDC, bin, cfgFile, statusFile, zap.L())
		srv := webapi.NewServer(s, webHost, webPort, dataDir, StaticFS, cdcSup, statusFile, time.Duration(cdcStaleSec)*time.Second)
		srv.SetCDCStatusProvider(webapi.NewFileCDCStatusProvider(statusFile, time.Duration(cdcStaleSec)*time.Second))
		fmt.Fprintf(os.Stderr, "pg2tidb web UI: http://%s:%d\n", webHost, webPort)
		return srv.Start()
	},
}

func init() {
	rootCmd.AddCommand(webCmd)
	webCmd.Flags().IntVarP(&webPort, "port", "p", 8080, "web server port")
	webCmd.Flags().StringVar(&webHost, "host", "0.0.0.0", "web server host")
	webCmd.Flags().StringVar(&webData, "data", ".pg2tidb", "data directory for SQLite store")
	webCmd.Flags().StringVar(&cdcStatusFile, "cdc-status-file", "", "CDC status JSON the dashboard reads (defaults to <data>/cdc/status.json; must match the CDC process --status-file — #t48 B)")
	webCmd.Flags().IntVar(&cdcStaleSec, "cdc-stale-threshold", 30, "seconds before CDC status is considered stale (~2-3x the CDC status write cadence)")
}

// StaticFS holds embedded frontend files. Populated via go:embed in static.go.
var StaticFS embed.FS
