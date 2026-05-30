package cmd

import (
	"embed"
	"fmt"
	"os"

	"github.com/pg2tidb/pg2tidb-migrator/internal/store"
	"github.com/pg2tidb/pg2tidb-migrator/internal/webapi"
	"github.com/spf13/cobra"
)

var (
	webPort int
	webHost string
	webData string
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

		srv := webapi.NewServer(s, webHost, webPort, StaticFS)
		fmt.Fprintf(os.Stderr, "pg2tidb web UI: http://%s:%d\n", webHost, webPort)
		return srv.Start()
	},
}

func init() {
	rootCmd.AddCommand(webCmd)
	webCmd.Flags().IntVarP(&webPort, "port", "p", 8080, "web server port")
	webCmd.Flags().StringVar(&webHost, "host", "0.0.0.0", "web server host")
	webCmd.Flags().StringVar(&webData, "data", ".pg2tidb", "data directory for SQLite store")
}

// StaticFS holds embedded frontend files. Populated via go:embed in static.go.
var StaticFS embed.FS
