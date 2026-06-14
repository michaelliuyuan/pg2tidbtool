package cmd

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pg2tidb/pg2tidb-migrator/internal/store"
	"github.com/pg2tidb/pg2tidb-migrator/internal/webapi"
	"github.com/spf13/cobra"
)

var (
	webPort          int
	webHost          string
	webData          string
	cdcStatusFile    string
	cdcStaleSec      int
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

		srv := webapi.NewServer(s, webHost, webPort, dataDir, StaticFS)
		// CDC dashboard (#t48 B): read the CDC process's status file. Default is
		// <data-dir>/cdc/status.json (shared with CDC's --data-dir). Log the
		// resolved absolute path so a CDC/web cwd mismatch is VISIBLE, not a
		// silent always-not_running.
		statusFile := cdcStatusFile
		if statusFile == "" {
			statusFile = filepath.Join(dataDir, "cdc", "status.json")
		}
		if abs, err := filepath.Abs(statusFile); err == nil {
			fmt.Fprintf(os.Stderr, "cdc status file (dashboard reads): %s\n", abs)
		}
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
