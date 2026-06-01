package data

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	lightningpkg "github.com/pg2tidb/pg2tidb-migrator/internal/lightning"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/checkpoint"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/progress"
	"go.uber.org/zap"
)

var chunkFileIndexRegexp = regexp.MustCompile(`^(.+)\.(\d+)$`)

type Migrator struct {
	cfg       config.Config
	pgDB      *sql.DB
	cpMgr     *checkpoint.Manager
	display   *progress.Display
}

func NewMigrator(cfg config.Config) *Migrator {
	return &Migrator{cfg: cfg}
}

func (m *Migrator) Run(ctx context.Context, opts common.DataOpts) (*common.DataResult, error) {
	logger := zap.L()
	startTime := time.Now()

	logger.Info("starting data migration",
		zap.Int("parallel", opts.Parallel),
		zap.Int("batch_size", opts.BatchSize),
		zap.Bool("use_lightning", opts.UseLightning))

	var err error
	m.pgDB, err = sql.Open("pgx", m.cfg.Source.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "connect to PostgreSQL", err)
	}
	defer m.pgDB.Close()

	m.pgDB.SetMaxOpenConns(opts.Parallel + 2)
	m.pgDB.SetConnMaxLifetime(10 * time.Minute)
	m.pgDB.SetConnMaxIdleTime(5 * time.Minute)

	if err := m.pgDB.PingContext(ctx); err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "ping PostgreSQL", err)
	}

	cpDir := m.cfg.Migration.CheckpointDir
	m.cpMgr, err = checkpoint.NewManager(cpDir)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrCheckpointLoad, "init checkpoint", err)
	}
	m.cpMgr.SetPhase("data-export")

	if err := os.MkdirAll(opts.TempDir, 0755); err != nil {
		return nil, cerrors.Wrap(cerrors.ErrDataExport, "create temp dir", err)
	}

	tables, err := m.getTables(ctx, opts.Tables, opts.ExcludeTables)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrDataExport, "get table list", err)
	}

	logger.Info("migrating tables", zap.Int("count", len(tables)))

	var totalRows atomic.Int64
	var totalBytes atomic.Int64

	if !opts.UseLightning {
		// Streaming INSERT path: skip CSV export, import directly via SQL
		if err := m.importViaSQL(ctx, opts); err != nil {
			return nil, cerrors.Wrap(cerrors.ErrDataImport, "sql import", err)
		}
	} else {
		// CSV export + LOAD DATA path
		m.display = progress.NewDisplay()
		m.display.Start()

		sem := make(chan struct{}, opts.Parallel)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		for _, table := range tables {
			if m.cpMgr.IsTableCompleted(table) {
				logger.Info("skipping completed table", zap.String("table", table))
				continue
			}

			rowCount, err := m.getRowCount(ctx, table)
			if err != nil {
				logger.Warn("failed to get row count", zap.String("table", table), zap.Error(err))
				rowCount = 0
			}

			m.cpMgr.GetOrCreateTable(table, rowCount)
			bar := m.display.AddBar(table, rowCount)

			threshold := m.cfg.Migration.LargeTableThreshold
			if threshold <= 0 {
				threshold = 1000000
			}

			sem <- struct{}{}
			wg.Add(1)

			go func(tableName string, bar *progress.Bar, rowCount int64) {
				defer wg.Done()
				defer func() { <-sem }()

				m.cpMgr.MarkTableRunning(tableName)

				var rows int64
				var bytes int64
				var exportErr error

				if rowCount > threshold {
					rows, bytes, exportErr = m.exportTableChunked(ctx, tableName, rowCount, opts)
				} else {
					rows, bytes, exportErr = m.exportTable(ctx, tableName, opts)
				}

				if exportErr != nil {
					m.cpMgr.MarkTableFailed(tableName, exportErr.Error())
					m.display.RemoveBar(tableName)
					errMu.Lock()
					if firstErr == nil {
						firstErr = cerrors.WithTable(
							cerrors.Wrap(cerrors.ErrDataExport, "export table", exportErr),
							tableName)
					}
					errMu.Unlock()
					return
				}

				bar.Set(rows)
				totalRows.Add(rows)
				totalBytes.Add(bytes)
				m.cpMgr.MarkTableCompleted(tableName, rows)
				m.display.RemoveBar(tableName)

				logger.Info("table exported",
					zap.String("table", tableName),
					zap.Int64("rows", rows),
					zap.Int64("bytes", bytes))
			}(table, bar, rowCount)
		}

		wg.Wait()
		logger.Info("[DEBUG] all table exports finished, stopping display")
		m.display.Stop()
		logger.Info("[DEBUG] display stopped, flushing checkpoint")
		m.cpMgr.Flush()
		logger.Info("[DEBUG] checkpoint flushed")

		if firstErr != nil && m.cfg.Migration.OnError != "skip" {
			return nil, firstErr
		}

		logger.Info("[DEBUG] setting phase to data-import")
		m.cpMgr.SetPhase("data-import")
		logger.Info("[DEBUG] resetting all tables")
		m.cpMgr.ResetAllTables()
		logger.Info("[DEBUG] flushing checkpoint again")
		m.cpMgr.Flush()
		logger.Info("[DEBUG] starting Lightning import")

		if err := m.importViaLightning(ctx, opts, tables); err != nil {
			logger.Warn("LOAD DATA import failed, falling back to streaming INSERT", zap.Error(err))
			if err := m.importViaSQL(ctx, opts); err != nil {
				return nil, cerrors.Wrap(cerrors.ErrDataImport, "sql import", err)
			}
		} else {
			// Lightning succeeded: mark all tables as completed in checkpoint
			for _, table := range tables {
				tc := m.cpMgr.GetOrCreateTable(table, 0)
				m.cpMgr.MarkTableCompleted(table, tc.RowsTotal)
			}
		}
	}

	duration := time.Since(startTime)
	result := &common.DataResult{
		TotalRows:   totalRows.Load(),
		TotalTables: len(tables),
		TotalBytes:  totalBytes.Load(),
		Duration:    formatDuration(duration),
		ExportPath:  opts.TempDir,
	}

	logger.Info("data migration completed",
		zap.Int64("total_rows", result.TotalRows),
		zap.Int("tables", result.TotalTables),
		zap.String("duration", result.Duration))

	return result, nil
}

func (m *Migrator) getTables(ctx context.Context, include, exclude []string) ([]string, error) {
	if len(include) > 0 {
		return include, nil
	}

	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := m.pgDB.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !contains(exclude, name) {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

func (m *Migrator) getRowCount(ctx context.Context, table string) (int64, error) {
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}
	var count int64
	err := m.pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quotePG(schema), quotePG(table))).Scan(&count)
	return count, err
}

func (m *Migrator) exportTable(ctx context.Context, table string, opts common.DataOpts) (int64, int64, error) {
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	outputPath := filepath.Join(opts.TempDir, table+".csv")
	f, err := os.Create(outputPath)
	if err != nil {
		return 0, 0, fmt.Errorf("create csv file: %w", err)
	}
	defer f.Close()

	var totalRows int64
	copyQuery := fmt.Sprintf("COPY %s.%s TO STDOUT WITH (FORMAT csv, NULL '\\N', HEADER false)",
		quotePG(schema), quotePG(table))

	conn, err := m.pgDB.Conn(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()

	exportErr := m.exportTableFallback(ctx, schema, table, f, opts, &totalRows)
	if exportErr != nil {
		err = conn.Raw(func(driverConn interface{}) error {
			pgConn, ok := driverConn.(interface {
				CopyTo(context.Context, string, string) (int64, error)
			})
			if !ok {
				return exportErr
			}
			n, copyErr := pgConn.CopyTo(ctx, copyQuery, "")
			totalRows = n
			return copyErr
		})
		if err != nil {
			return totalRows, 0, fmt.Errorf("copy export: %w", err)
		}
	}

	fi, _ := f.Stat()
	var totalBytes int64
	if fi != nil {
		totalBytes = fi.Size()
	}

	return totalRows, totalBytes, nil
}

func (m *Migrator) exportTableFallback(ctx context.Context, schema, table string, f *os.File, opts common.DataOpts, totalRows *int64) error {
	selectCols, err := m.buildSelectCols(ctx, schema, table)
	if err != nil {
		return fmt.Errorf("build select columns: %w", err)
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s", selectCols, quotePG(schema), quotePG(table))
	rows, err := m.pgDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query table: %w", err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	bw := bufio.NewWriterSize(f, 256*1024)
	var rowCount int64
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		record := make([]string, len(cols))
		for i, val := range values {
			record[i] = convertValue(val)
		}
		line := strings.Join(record, "\t") + "\n"
		if _, err := bw.WriteString(line); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
		rowCount++
		if rowCount%int64(opts.BatchSize) == 0 {
			*totalRows = rowCount
			m.cpMgr.UpdateTableProgress(table, rowCount, 0)
		}
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush csv writer: %w", err)
	}
	*totalRows = rowCount
	return nil
}

func (m *Migrator) importViaLightning(ctx context.Context, opts common.DataOpts, tables []string) error {
	logger := zap.L()
	logger.Info("TiDB Lightning import starting", zap.String("dir", opts.TempDir))

	entries, err := os.ReadDir(opts.TempDir)
	if err != nil {
		return err
	}

	hasCSV := false
	chunkedTables := make(map[string]int)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv") {
			hasCSV = true
			if idx := chunkFileIndexRegexp.FindStringSubmatch(strings.TrimSuffix(entry.Name(), ".csv")); idx != nil {
				chunkedTables[idx[1]]++
			}
		}
	}
	if !hasCSV {
		logger.Warn("no CSV files found in temp dir, skipping Lightning import")
		return nil
	}

	for tbl, cnt := range chunkedTables {
		logger.Info("chunked table detected for Lightning import",
			zap.String("table", tbl),
			zap.Int("chunks", cnt))
	}

	absDir, err := filepath.Abs(opts.TempDir)
	if err != nil {
		return fmt.Errorf("get absolute path: %w", err)
	}

	// Rename CSV files from {table}.csv to {database}.{table}.csv for Lightning file router
	// Chunked files: {table}.0.csv → {database}.{table}.000.csv
	// Single files: {table}.csv → {database}.{table}.csv
	targetDB := m.cfg.Target.Database
	if targetDB == "" {
		targetDB = "test"
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		// Skip if already has database prefix
		if strings.Count(entry.Name(), ".") >= 3 {
			continue
		}

		name := entry.Name()
		baseName := strings.TrimSuffix(name, ".csv")

		var newName string
		if idx := chunkFileIndexRegexp.FindStringSubmatch(baseName); idx != nil {
			// Chunked file: {table}.{chunk_index} → {database}.{table}.{chunk_index_padded}.csv
			tableName := idx[1]
			chunkIdx := idx[2]
			newName = fmt.Sprintf("%s.%s.%s.csv", targetDB, tableName, chunkIdx)
		} else if strings.Count(name, ".") >= 2 {
			continue
		} else {
			// Single file: {table} → {database}.{table}.csv
			newName = targetDB + "." + baseName + ".csv"
		}
		oldPath := filepath.Join(absDir, entry.Name())
		newPath := filepath.Join(absDir, newName)
		if err := os.Rename(oldPath, newPath); err != nil {
			logger.Warn("failed to rename CSV file for Lightning", zap.String("old", entry.Name()), zap.String("new", newName), zap.Error(err))
		} else {
			logger.Info("renamed CSV for Lightning", zap.String("old", entry.Name()), zap.String("new", newName))
		}
	}

	lightningBin := lightningpkg.FindBinary(m.cfg.Migration.TempDir)
	if lightningBin == "" {
		return fmt.Errorf("tidb-lightning not found: install tidb-lightning or use a build with embedded binary")
	}

	// Defaults for PD address and status port
	pdAddr := m.cfg.Target.PDAddr
	if pdAddr == "" {
		pdAddr = fmt.Sprintf("%s:2379", m.cfg.Target.Host)
	}
	statusPort := m.cfg.Target.StatusPort
	if statusPort == 0 {
		statusPort = 10080
	}
	sortedKVDir := filepath.Join(absDir, ".sorted-kv")
	if err := os.MkdirAll(sortedKVDir, 0755); err != nil {
		return fmt.Errorf("create sorted-kv dir: %w", err)
	}
	// Clean up old Lightning checkpoints to avoid "illegal checkpoints" errors
	os.Remove(filepath.Join(sortedKVDir, "tidb_lightning_checkpoint.pb"))
	os.Remove(filepath.Join(absDir, "tidb_lightning_checkpoint.pb"))

	// Apply target policy before Lightning import (only truncate; drop is handled by schema migration)
	if m.cfg.Migration.TargetPolicy == "truncate" {
		logger.Info("applying target policy before Lightning import",
			zap.String("policy", m.cfg.Migration.TargetPolicy),
			zap.Int("tables", len(tables)))
		tidbDSN := m.cfg.Target.DSN()
		tidbDB, openErr := sql.Open("mysql", tidbDSN)
		if openErr != nil {
			return fmt.Errorf("connect to TiDB for target policy: %w", openErr)
		}
		defer tidbDB.Close()
		if pingErr := tidbDB.PingContext(ctx); pingErr != nil {
			return fmt.Errorf("ping TiDB for target policy: %w", pingErr)
		}
		if applyErr := m.applyTargetPolicy(ctx, tidbDB, tables); applyErr != nil {
			return fmt.Errorf("apply target policy: %w", applyErr)
		}
	}

	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		baseName := strings.TrimSuffix(entry.Name(), ".csv")
		if idx := chunkFileIndexRegexp.FindStringSubmatch(baseName); idx != nil {
			baseName = idx[1]
		}
		if _, ok := tableSet[baseName]; !ok {
			removePath := filepath.Join(absDir, entry.Name())
			logger.Info("removing CSV for non-selected table", zap.String("file", entry.Name()), zap.String("table", baseName))
			os.Remove(removePath)
		}
	}

	configContent := fmt.Sprintf(`[lightning]
	level = "info"
check-requirements = false

[mydumper]
data-source-dir = "%s"
no-schema = true

[mydumper.csv]
separator = "\t"
delimiter = ""
header = false
not-null = false
null = "\\N"
backslash-escape = false
trim-last-separator = false

[tikv-importer]
backend = "local"
sorted-kv-dir = "%s"

[tidb]
host = "%s"
port = %d
user = "%s"
password = "%s"
status-port = %d
pd-addr = "%s"

[post-restore]
checksum = "optional"
analyze = "off"
`,
		strings.ReplaceAll(absDir, "\\", "/"),
		strings.ReplaceAll(sortedKVDir, "\\", "/"),
		m.cfg.Target.Host,
		m.cfg.Target.Port,
		m.cfg.Target.User,
		m.cfg.Target.Password,
		statusPort,
		pdAddr,
	)

	if m.cfg.Target.Password == "" {
		configContent = fmt.Sprintf(`[lightning]
level = "info"
check-requirements = false

[mydumper]
data-source-dir = "%s"
no-schema = true

[mydumper.csv]
separator = "\t"
delimiter = ""
header = false
not-null = false
null = "\\N"
backslash-escape = false
trim-last-separator = false

[tikv-importer]
backend = "local"
sorted-kv-dir = "%s"

[tidb]
host = "%s"
port = %d
user = "%s"
status-port = %d
pd-addr = "%s"

[post-restore]
checksum = "optional"
analyze = "off"
`,
			strings.ReplaceAll(absDir, "\\", "/"),
			strings.ReplaceAll(sortedKVDir, "\\", "/"),
			m.cfg.Target.Host,
			m.cfg.Target.Port,
			m.cfg.Target.User,
			statusPort,
			pdAddr,
		)
	}

	configPath := filepath.Join(absDir, "lightning.toml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		return fmt.Errorf("write lightning config: %w", err)
	}
	defer os.Remove(configPath)

	logger.Info("generated Lightning config",
		zap.String("config", configPath),
		zap.String("data_dir", absDir),
		zap.String("tidb_host", m.cfg.Target.Host),
		zap.Int("tidb_port", m.cfg.Target.Port))

	cmd := exec.CommandContext(ctx, lightningBin, "--config", configPath)
	cmd.Dir = absDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start tidb-lightning: %w", err)
	}

	var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	var srcPathRe = regexp.MustCompile(`\([^)]*\.go:\d+\)`)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = ansiRe.ReplaceAllString(line, "")
		line = srcPathRe.ReplaceAllString(line, "")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Contains(line, "[ERROR]") || strings.Contains(line, "[FATAL]") {
			logger.Error("lightning: " + line)
		} else if strings.Contains(line, "[WARN]") {
			logger.Warn("lightning: " + line)
		} else {
			logger.Info("lightning: " + line)
		}
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("tidb-lightning failed: %w", err)
	}

	logger.Info("TiDB Lightning import completed successfully")
	return nil
}

func isBadConnection(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "invalid connection") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "EOF")
}

func (m *Migrator) applyTargetPolicy(ctx context.Context, tidbDB *sql.DB, tables []string) error {
	policy := m.cfg.Migration.TargetPolicy
	if policy == "" || policy == "insert" {
		return nil
	}

	logger := zap.L()
	logger.Info("applying target data policy", zap.String("policy", policy), zap.Int("tables", len(tables)))

	targetDB := m.cfg.Target.Database
	if targetDB == "" {
		targetDB = "test"
	}

	var firstErr error
	for _, table := range tables {
		switch policy {
		case "truncate":
			logger.Info("truncating table", zap.String("table", table))
			_, err := tidbDB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", quoteMySQL(targetDB), quoteMySQL(table)))
			if err != nil {
				logger.Warn("truncate failed", zap.String("table", table), zap.Error(err))
				if firstErr == nil {
					firstErr = fmt.Errorf("truncate %s: %w", table, err)
				}
			}
		case "drop":
			logger.Info("truncating table (drop policy maps to truncate in data phase)", zap.String("table", table))
			_, err := tidbDB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", quoteMySQL(targetDB), quoteMySQL(table)))
			if err != nil {
				logger.Warn("drop failed", zap.String("table", table), zap.Error(err))
				if firstErr == nil {
					firstErr = fmt.Errorf("drop %s: %w", table, err)
				}
			}
		}
	}
	return firstErr
}

func (m *Migrator) ensureTablesExist(ctx context.Context, tidbDB *sql.DB, pgSchema string, tables []string) error {
	logger := zap.L()
	for _, table := range tables {
		var count int
		err := tidbDB.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
			table).Scan(&count)
		if err != nil {
			return fmt.Errorf("check table %s: %w", table, err)
		}
		if count > 0 {
			continue
		}

		logger.Info("table does not exist in target, creating from source schema", zap.String("table", table))

		rows, err := m.pgDB.QueryContext(ctx,
			`SELECT column_name, data_type, udt_name, is_nullable, column_default,
			        character_maximum_length, numeric_precision, numeric_scale
			 FROM information_schema.columns
			 WHERE table_schema = $1 AND table_name = $2
			 ORDER BY ordinal_position`, pgSchema, table)
		if err != nil {
			logger.Warn("failed to get source columns", zap.String("table", table), zap.Error(err))
			continue
		}

		type colInfo struct {
			Name       string
			DataType   string
			UDTName    string
			IsNullable string
		}
		var columns []colInfo
		for rows.Next() {
			var c colInfo
			var maxLen, numPrec, numScale sql.NullInt64
			var colDefault sql.NullString
			if err := rows.Scan(&c.Name, &c.DataType, &c.UDTName, &c.IsNullable, &colDefault, &maxLen, &numPrec, &numScale); err != nil {
				rows.Close()
				return err
			}
			columns = append(columns, c)
		}
		rows.Close()

		if len(columns) == 0 {
			continue
		}

		var colDefs []string
		for _, c := range columns {
			myType := pgTypeToMySQL(c.DataType, c.UDTName)
			nullStr := "NULL"
			if c.IsNullable == "NO" {
				nullStr = "NOT NULL"
			}
			colDefs = append(colDefs, fmt.Sprintf("%s %s %s", quoteMySQL(c.Name), myType, nullStr))
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteMySQL(table), strings.Join(colDefs, ", "))
		if _, err := tidbDB.ExecContext(ctx, ddl); err != nil {
			logger.Warn("failed to create table", zap.String("table", table), zap.Error(err))
		}
	}
	return nil
}

func pgTypeToMySQL(dataType, udtName string) string {
	if strings.HasPrefix(udtName, "_") || dataType == "ARRAY" {
		return "JSON"
	}
	switch dataType {
	case "integer", "int", "int4", "smallint", "int2":
		return "INT"
	case "bigint", "int8":
		return "BIGINT"
	case "serial":
		return "INT AUTO_INCREMENT"
	case "bigserial":
		return "BIGINT AUTO_INCREMENT"
	case "real", "float4":
		return "FLOAT"
	case "double precision", "float8":
		return "DOUBLE"
	case "numeric", "decimal":
		return "DECIMAL(65,30)"
	case "character varying", "varchar", "character", "char", "text":
		return "TEXT"
	case "boolean", "bool":
		return "TINYINT(1)"
	case "date":
		return "DATE"
	case "timestamp", "timestamp without time zone":
		return "DATETIME"
	case "timestamp with time zone", "timestamptz":
		return "DATETIME"
	case "time", "time without time zone":
		return "TIME"
	case "bytea":
		return "BLOB"
	case "json", "jsonb":
		return "JSON"
	case "uuid":
		return "CHAR(36)"
	case "interval":
		return "VARCHAR(64)"
	case "bit", "bit varying":
		return "BLOB"
	case "oid":
		return "BIGINT"
	case "money":
		return "DECIMAL(19,2)"
	case "inet":
		return "VARCHAR(45)"
	case "macaddr":
		return "VARCHAR(17)"
	case "point", "line", "lseg", "box", "path", "polygon", "circle":
		return "TEXT"
	case "tsvector", "tsquery":
		return "TEXT"
	case "xml":
		return "LONGTEXT"
	case "user-defined":
		return "TEXT"
	default:
		return "TEXT"
	}
}

func (m *Migrator) importViaSQL(ctx context.Context, opts common.DataOpts) error {
	logger := zap.L()
	logger.Info("starting streaming SQL import (batch INSERT)")

	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	tables, err := m.getTables(ctx, opts.Tables, opts.ExcludeTables)
	if err != nil {
		return fmt.Errorf("get table list: %w", err)
	}

	tidbDB, err := sql.Open("mysql", m.cfg.Target.DSN())
	if err != nil {
		return err
	}
	defer tidbDB.Close()

	tidbDB.SetConnMaxLifetime(5 * time.Minute)
	tidbDB.SetConnMaxIdleTime(2 * time.Minute)
	parallel := opts.Parallel
	if parallel <= 0 {
		parallel = 4
	}
	tidbDB.SetMaxOpenConns(parallel + 1)

	if err := tidbDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping TiDB: %w", err)
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}
	if batchSize > 5000 {
		batchSize = 5000
	}

	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, table := range tables {
		rowCount, err := m.getRowCount(ctx, table)
		if err != nil {
			logger.Warn("failed to get row count", zap.String("table", table), zap.Error(err))
			rowCount = 0
		}
		m.cpMgr.GetOrCreateTable(table, rowCount)
		m.cpMgr.MarkTableRunning(table)

		sem <- struct{}{}
		wg.Add(1)

		go func(tableName string, estimatedRows int64) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := m.streamTable(ctx, tidbDB, schema, tableName, batchSize, estimatedRows); err != nil {
				m.cpMgr.MarkTableFailed(tableName, err.Error())
				if m.cfg.Migration.OnError != "skip" {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("stream table %s: %w", tableName, err)
					}
					errMu.Unlock()
					return
				}
				logger.Warn("table stream error", zap.String("table", tableName), zap.Error(err))
				return
			}
		}(table, rowCount)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	return nil
}

func (m *Migrator) streamTable(ctx context.Context, tidbDB *sql.DB, schema, table string, batchSize int, estimatedRows int64) error {
	logger := zap.L()
	logger.Info("streaming table to TiDB", zap.String("table", table))

	rowCount := estimatedRows
	if rowCount > 0 {
		logger.Info("table row count", zap.String("table", table), zap.Int64("rows", rowCount))
	}
	if rowCount == 0 {
		rc, _ := m.getRowCount(ctx, table)
		rowCount = rc
		if rowCount > 0 {
			m.cpMgr.UpdateTable(table, func(tc *checkpoint.TableCheckpoint) {
				tc.RowsTotal = rowCount
			})
		}
	}

	// Use a separate PG connection for this table
	pgConn, err := m.pgDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get pg connection: %w", err)
	}
	defer pgConn.Close()

	selectQuery := fmt.Sprintf("SELECT * FROM %s.%s", quotePG(schema), quotePG(table))
	rows, err := pgConn.QueryContext(ctx, selectQuery)
	if err != nil {
		return fmt.Errorf("query %s: %w", table, err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("get columns for %s: %w", table, err)
	}

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = quoteMySQL(col.Name())
	}
	colList := strings.Join(colNames, ", ")
	placeholders := strings.Repeat("?,", len(cols))
	placeholders = placeholders[:len(placeholders)-1]

	insertBase := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteMySQL(table), colList, placeholders)

	var batch [][]interface{}
	totalRows := 0

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			logger.Warn("scan error", zap.String("table", table), zap.Error(err))
			continue
		}

		converted := make([]interface{}, len(cols))
		for i, v := range values {
			converted[i] = convertSQLValue(v)
		}
		batch = append(batch, converted)

		if len(batch) >= batchSize {
			if err := m.execBatch(ctx, tidbDB, insertBase, batch, len(cols)); err != nil {
				if m.cfg.Migration.OnError != "skip" {
					return fmt.Errorf("insert batch for %s: %w", table, err)
				}
				logger.Warn("batch insert error", zap.String("table", table), zap.Error(err))
				batch = batch[:0]
				continue
			}
			totalRows += len(batch)
			m.cpMgr.UpdateTableProgress(table, int64(totalRows), 0)
			logger.Info("batch inserted", zap.String("table", table), zap.Int("rows_in_batch", totalRows), zap.Int64("total", rowCount))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := m.execBatch(ctx, tidbDB, insertBase, batch, len(cols)); err != nil {
			if m.cfg.Migration.OnError != "skip" {
				return fmt.Errorf("insert final batch for %s: %w", table, err)
			}
			logger.Warn("final batch error", zap.String("table", table), zap.Error(err))
		} else {
			totalRows += len(batch)
		}
	}

	m.cpMgr.MarkTableCompleted(table, int64(totalRows))
	logger.Info("table import completed", zap.String("table", table), zap.Int("rows", totalRows))
	return nil
}

func (m *Migrator) execBatch(ctx context.Context, db *sql.DB, insertBase string, batch [][]interface{}, colCount int) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			lastErr = err
			continue
		}

		stmt, err := tx.PrepareContext(ctx, insertBase)
		if err != nil {
			tx.Rollback()
			lastErr = err
			continue
		}

		batchErr := func() error {
			for _, row := range batch {
				if _, err := stmt.ExecContext(ctx, row...); err != nil {
					return err
				}
			}
			return nil
		}()

		stmt.Close()

		if batchErr != nil {
			tx.Rollback()
			lastErr = batchErr
			if !isBadConnection(batchErr) {
				return batchErr
			}
			zap.L().Warn("bad connection in batch, retrying", zap.Int("attempt", attempt+1))
			continue
		}

		if err := tx.Commit(); err != nil {
			lastErr = err
			if !isBadConnection(err) {
				return err
			}
			continue
		}
		return nil
	}
	return lastErr
}

func convertSQLValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []byte:
		return tryConvertArray(string(v))
	case string:
		return tryConvertArray(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999")
	default:
		return v
	}
}

func tryConvertArray(s string) interface{} {
	if isPGArray(s) {
		return pgArrayToJSON(s)
	}
	return s
}

func isPGArray(s string) bool {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return false
	}
	return true
}

func pgArrayToJSON(s string) string {
	inner := s[1 : len(s)-1]
	if inner == "" {
		return "[]"
	}

	elements := splitPGArrayElements(inner)
	parts := make([]string, 0, len(elements))
	for _, elem := range elements {
		elem = strings.TrimSpace(elem)
		if elem == "" {
			parts = append(parts, "null")
		} else if elem == "NULL" || elem == "null" {
			parts = append(parts, "null")
		} else if elem == "t" {
			parts = append(parts, "true")
		} else if elem == "f" {
			parts = append(parts, "false")
		} else if len(elem) >= 2 && elem[0] == '"' && elem[len(elem)-1] == '"' {
			unquoted := elem[1 : len(elem)-1]
			unquoted = strings.ReplaceAll(unquoted, `\"`, `"`)
			unquoted = strings.ReplaceAll(unquoted, `\\`, `\`)
			b, _ := json.Marshal(unquoted)
			parts = append(parts, string(b))
		} else if elem[0] == '{' {
			parts = append(parts, pgArrayToJSON(elem))
		} else {
			if _, err := strconv.ParseFloat(elem, 64); err == nil {
				parts = append(parts, elem)
			} else {
				b, _ := json.Marshal(elem)
				parts = append(parts, string(b))
			}
		}
	}

	return "[" + strings.Join(parts, ",") + "]"
}

func splitPGArrayElements(s string) []string {
	var elements []string
	current := ""
	inQuote := false
	escape := false
	depth := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if escape {
			current += string(ch)
			escape = false
			continue
		}
		if ch == '\\' {
			escape = true
			current += string(ch)
			continue
		}
		if ch == '"' {
			inQuote = !inQuote
			current += string(ch)
			continue
		}
		if ch == '{' && !inQuote {
			depth++
			current += string(ch)
		} else if ch == '}' && !inQuote {
			depth--
			current += string(ch)
		} else if ch == ',' && !inQuote && depth == 0 {
			elements = append(elements, current)
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" || len(elements) > 0 {
		elements = append(elements, current)
	}
	return elements
}

func convertValue(val interface{}) string {
	if val == nil {
		return "\\N"
	}
	switch v := val.(type) {
	case bool:
		if v {
			return "1"
		}
		return "0"
	case []byte:
		s := string(v)
		// Convert PG array format {1,2,3} to JSON [1,2,3]
		if len(s) > 1 && s[0] == '{' && s[len(s)-1] == '}' {
			return pgArrayToJSON(s)
		}
		return s
	case string:
		// Convert PG array format {1,2,3} to JSON [1,2,3]
		if len(v) > 1 && v[0] == '{' && v[len(v)-1] == '}' {
			return pgArrayToJSON(v)
		}
		return v
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999")
	case fmt.Stringer:
		s := v.String()
		if len(s) > 1 && s[0] == '{' && s[len(s)-1] == '}' {
			return pgArrayToJSON(s)
		}
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}

func quotePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (m *Migrator) buildSelectCols(ctx context.Context, schema, table string) (string, error) {
	rows, err := m.pgDB.QueryContext(ctx, `
		SELECT column_name, udt_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return "*", nil
	}
	defer rows.Close()

	type colInfo struct {
		Name    string
		UDTName string
	}
	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.Name, &c.UDTName); err != nil {
			return "*", nil
		}
		cols = append(cols, c)
	}
	if len(cols) == 0 {
		return "*", nil
	}

	var selectParts []string
	for _, c := range cols {
		if strings.HasPrefix(c.UDTName, "_") {
			// PostgreSQL array type: convert to JSON
			selectParts = append(selectParts,
				fmt.Sprintf("CASE WHEN %s IS NULL THEN NULL ELSE array_to_json(%s)::text END AS %s",
					quotePG(c.Name), quotePG(c.Name), quotePG(c.Name)))
		} else {
			selectParts = append(selectParts, quotePG(c.Name))
		}
	}
	return strings.Join(selectParts, ", "), nil
}

type ChunkBoundary struct {
	Index    int
	MinValue interface{}
	MaxValue interface{}
	IsLast   bool
}

func (m *Migrator) getPrimaryKeyInfo(ctx context.Context, schema, table string) (pkColumn string, pkType string, err error) {
	query := `
		SELECT kcu.column_name, c.data_type
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.columns c
			ON kcu.table_schema = c.table_schema
			AND kcu.table_name = c.table_name
			AND kcu.column_name = c.column_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY kcu.ordinal_position
		LIMIT 1
	`
	err = m.pgDB.QueryRowContext(ctx, query, schema, table).Scan(&pkColumn, &pkType)
	if err != nil {
		return "", "", err
	}
	return pkColumn, pkType, nil
}

func (m *Migrator) calculateChunkBoundaries(
	ctx context.Context,
	schema, table, pkColumn string,
	totalRows, chunkSize int64,
) ([]ChunkBoundary, error) {
	logger := zap.L()

	if chunkSize <= 0 {
		chunkSize = 500000
	}
	numChunks := (totalRows + chunkSize - 1) / chunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	var minVal, maxVal int64
	rangeQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s.%s",
		quotePG(pkColumn), quotePG(pkColumn), quotePG(schema), quotePG(table))
	if err := m.pgDB.QueryRowContext(ctx, rangeQuery).Scan(&minVal, &maxVal); err != nil {
		logger.Warn("failed to get PK range, falling back to OFFSET/LIMIT",
			zap.String("table", table), zap.Error(err))
		return m.calculateChunkBoundariesByOffset(numChunks, chunkSize), nil
	}

	rangeSize := maxVal - minVal + 1
	chunkRange := rangeSize / numChunks
	if chunkRange == 0 {
		chunkRange = 1
	}

	var boundaries []ChunkBoundary
	for i := int64(0); i < numChunks; i++ {
		bMin := minVal + i*chunkRange
		bMax := minVal + (i+1)*chunkRange
		isLast := i == numChunks-1
		if isLast {
			bMax = maxVal + 1
		}
		boundaries = append(boundaries, ChunkBoundary{
			Index:    int(i),
			MinValue: bMin,
			MaxValue: bMax,
			IsLast:   isLast,
		})
	}

	return boundaries, nil
}

func (m *Migrator) calculateChunkBoundariesByOffset(numChunks, chunkSize int64) []ChunkBoundary {
	var boundaries []ChunkBoundary
	for i := int64(0); i < numChunks; i++ {
		boundaries = append(boundaries, ChunkBoundary{
			Index:    int(i),
			MinValue: i * chunkSize,
			MaxValue: 0,
			IsLast:   i == numChunks-1,
		})
	}
	return boundaries
}

func (m *Migrator) exportChunk(
	ctx context.Context,
	schema, table, pkColumn, selectCols string,
	boundary ChunkBoundary,
	chunkFile string,
	opts common.DataOpts,
	isOffsetMode bool,
) (int64, int64, error) {
	f, err := os.Create(chunkFile)
	if err != nil {
		return 0, 0, fmt.Errorf("create chunk file %s: %w", chunkFile, err)
	}
	defer f.Close()

	var query string
	var rows *sql.Rows

	if isOffsetMode {
		offset := boundary.MinValue.(int64)
		if boundary.IsLast {
			query = fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY ctid LIMIT %d OFFSET %d",
				selectCols, quotePG(schema), quotePG(table), opts.BatchSize*2, offset)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY ctid LIMIT %d OFFSET %d",
				selectCols, quotePG(schema), quotePG(table), opts.BatchSize*2, offset)
		}
		rows, err = m.pgDB.QueryContext(ctx, query)
	} else {
		if boundary.IsLast {
			query = fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s >= $1 ORDER BY %s",
				selectCols, quotePG(schema), quotePG(table), quotePG(pkColumn), quotePG(pkColumn))
			rows, err = m.pgDB.QueryContext(ctx, query, boundary.MinValue)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s >= $1 AND %s < $2 ORDER BY %s",
				selectCols, quotePG(schema), quotePG(table), quotePG(pkColumn), quotePG(pkColumn), quotePG(pkColumn))
			rows, err = m.pgDB.QueryContext(ctx, query, boundary.MinValue, boundary.MaxValue)
		}
	}

	if err != nil {
		return 0, 0, fmt.Errorf("query chunk %d: %w", boundary.Index, err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return 0, 0, err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	bw := bufio.NewWriterSize(f, 256*1024)
	var rowCount int64
	var byteCount int64
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return rowCount, byteCount, fmt.Errorf("scan row in chunk %d: %w", boundary.Index, err)
		}
		record := make([]string, len(cols))
		for i, val := range values {
			record[i] = convertValue(val)
		}
		line := strings.Join(record, "\t") + "\n"
		n, writeErr := bw.WriteString(line)
		if writeErr != nil {
			return rowCount, byteCount, fmt.Errorf("write row in chunk %d: %w", boundary.Index, writeErr)
		}
		byteCount += int64(n)
		rowCount++
	}

	if err := bw.Flush(); err != nil {
		return rowCount, byteCount, fmt.Errorf("flush chunk writer %d: %w", boundary.Index, err)
	}

	return rowCount, byteCount, nil
}

func (m *Migrator) exportTableChunked(
	ctx context.Context,
	table string,
	rowCount int64,
	opts common.DataOpts,
) (int64, int64, error) {
	logger := zap.L()
	schema := m.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	pkColumn, pkType, err := m.getPrimaryKeyInfo(ctx, schema, table)
	if err != nil {
		logger.Warn("table has no usable primary key, falling back to single file export",
			zap.String("table", table), zap.Error(err))
		return m.exportTable(ctx, table, opts)
	}

	isIntegerPK := pkType == "integer" || pkType == "bigint" || pkType == "smallint" ||
		pkType == "int" || pkType == "serial" || pkType == "bigserial"

	chunkSize := m.cfg.Migration.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 500000
	}

	var boundaries []ChunkBoundary
	var isOffsetMode bool

	if isIntegerPK {
		boundaries, err = m.calculateChunkBoundaries(ctx, schema, table, pkColumn, rowCount, chunkSize)
		if err != nil {
			return 0, 0, fmt.Errorf("calculate chunk boundaries for %s: %w", table, err)
		}
		isOffsetMode = false
	} else {
		numChunks := (rowCount + chunkSize - 1) / chunkSize
		if numChunks < 1 {
			numChunks = 1
		}
		boundaries = m.calculateChunkBoundariesByOffset(numChunks, chunkSize)
		isOffsetMode = true
	}

	logger.Info("exporting table in chunks",
		zap.String("table", table),
		zap.Int("chunks", len(boundaries)),
		zap.String("pk_column", pkColumn),
		zap.Bool("offset_mode", isOffsetMode))

	for _, b := range boundaries {
		m.cpMgr.InitChunk(table, b.Index)
	}

	chunkParallel := m.cfg.Migration.ChunkParallel
	if chunkParallel <= 0 {
		chunkParallel = 4
	}

	selectCols, err := m.buildSelectCols(ctx, schema, table)
	if err != nil {
		return 0, 0, fmt.Errorf("build select columns: %w", err)
	}

	var totalRows, totalBytes int64
	var mu sync.Mutex
	sem := make(chan struct{}, chunkParallel)
	var wg sync.WaitGroup
	var firstErr error

	for _, boundary := range boundaries {
		if m.cpMgr.IsChunkCompleted(table, boundary.Index) {
			rows, bytes := m.cpMgr.GetChunkProgress(table, boundary.Index)
			mu.Lock()
			totalRows += rows
			totalBytes += bytes
			mu.Unlock()
			continue
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(b ChunkBoundary) {
			defer wg.Done()
			defer func() { <-sem }()

			chunkFile := filepath.Join(opts.TempDir, fmt.Sprintf("%s.%d.csv", table, b.Index))
			rows, bytes, exportErr := m.exportChunk(ctx, schema, table, pkColumn, selectCols, b, chunkFile, opts, isOffsetMode)
			if exportErr != nil {
				m.cpMgr.MarkChunkFailed(table, b.Index, exportErr.Error())
				mu.Lock()
				if firstErr == nil {
					firstErr = exportErr
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			totalRows += rows
			totalBytes += bytes
			mu.Unlock()

			m.cpMgr.MarkChunkCompleted(table, b.Index, rows, bytes)
			m.cpMgr.UpdateTableProgress(table, totalRows, totalBytes)

			logger.Info("chunk exported",
				zap.String("table", table),
				zap.Int("chunk", b.Index),
				zap.Int64("rows", rows),
				zap.Int64("bytes", bytes))
		}(boundary)
	}

	wg.Wait()
	return totalRows, totalBytes, firstErr
}

func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := math.Mod(d.Seconds(), 60)
	if hours > 0 {
		return fmt.Sprintf("%dh%dm%.3fs", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%.3fs", minutes, seconds)
	}
	return fmt.Sprintf("%.3fs", seconds)
}
