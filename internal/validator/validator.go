package validator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/go-sql-driver/mysql"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/config"
	cerrors "github.com/pg2tidb/pg2tidb-migrator/internal/common/errors"
	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

type Validator struct {
	cfg config.Config
}

func NewValidator(cfg config.Config) *Validator {
	return &Validator{cfg: cfg}
}

// getTiDBConn gets a dedicated connection from the TiDB connection pool and
// sets the session timezone to UTC. This ensures TIMESTAMP values are returned
// in UTC, matching PostgreSQL's timestamptz output.
func getTiDBConn(ctx context.Context, tidbDB *sql.DB) (*sql.Conn, error) {
	conn, err := tidbDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get TiDB connection: %w", err)
	}
	_, err = conn.ExecContext(ctx, "SET time_zone = '+00:00'")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("set TiDB timezone: %w", err)
	}
	return conn, nil
}

func (v *Validator) Run(ctx context.Context, opts common.ValidateOpts) (*reporter.Report, error) {
	logger := zap.L()
	logger.Info("starting data validation", zap.String("level", opts.Level), zap.String("mode", opts.Mode))

	// Resolve effective mode: CLI flag > config default
	mode := opts.Mode
	if mode == "" {
		mode = v.cfg.Compare.CompareMode
	}
	if mode == "" {
		mode = "sample"
	}

	rpt := reporter.NewReport("data-validation")

	pgDB, err := sql.Open("pgx", v.cfg.Source.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrSourceConnect, "connect to PostgreSQL", err)
	}
	defer pgDB.Close()

	tidbDB, err := sql.Open("mysql", v.cfg.Target.DSN())
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrTargetConnect, "connect to TiDB", err)
	}
	defer tidbDB.Close()

	pgDB.SetMaxOpenConns(8)
	pgDB.SetConnMaxLifetime(5 * time.Minute)
	tidbDB.SetMaxOpenConns(8)
	tidbDB.SetConnMaxLifetime(5 * time.Minute)

	tables, err := v.getTables(ctx, pgDB, opts.Tables)
	if err != nil {
		return nil, cerrors.Wrap(cerrors.ErrValidateRowCount, "get table list", err)
	}

	parallel := v.cfg.Migration.Parallel
	if parallel <= 0 {
		parallel = 4
	}

	var mu sync.Mutex
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup

	for _, table := range tables {
		wg.Add(1)
		sem <- struct{}{}
		go func(tableName string) {
			defer wg.Done()
			defer func() { <-sem }()

			// Get a dedicated TiDB connection with UTC timezone for this goroutine.
			// This ensures all queries on this connection return TIMESTAMP values
			// in UTC, matching PostgreSQL's timestamptz output.
			tidbConn, connErr := getTiDBConn(ctx, tidbDB)
			if connErr != nil {
				mu.Lock()
				rpt.AddTableReport(reporter.TableReport{
					TableName: tableName,
					Status:    reporter.StatusFail,
					Error:     fmt.Sprintf("get TiDB connection: %v", connErr),
				})
				mu.Unlock()
				return
			}
			defer tidbConn.Close()

			var tr reporter.TableReport
			switch mode {
			case "quick":
				tr = v.validateRowCount(ctx, pgDB, tidbConn, tableName)
			case "sample":
				tr = v.validateSampling(ctx, pgDB, tidbConn, tidbDB, tableName, opts.SampleRatio)
			case "checksum":
				tr = v.validateChecksum(ctx, pgDB, tidbConn, tableName)
			default:
				tr = reporter.TableReport{
					TableName: tableName,
					Status:    reporter.StatusFail,
					Error:     fmt.Sprintf("unknown validation mode: %s", mode),
				}
			}

			mu.Lock()
			rpt.AddTableReport(tr)
			mu.Unlock()

			logger.Info("table validation result",
				zap.String("table", tableName),
				zap.String("status", string(tr.Status)),
				zap.Int64("diff", tr.DiffRows))
		}(table)
	}

	wg.Wait()

	// Log summary of failed/warned tables for visibility
	failTables := rpt.FailedTables()
	if len(failTables) > 0 {
		for _, t := range failTables {
			logger.Warn("table validation FAILED",
				zap.String("table", t.TableName),
				zap.String("error", t.Error),
				zap.Int64("diff", t.DiffRows))
		}
		logger.Warn("data validation summary",
			zap.Int("failed", len(failTables)),
			zap.Int("total", len(tables)))
	}

	rpt.Finish(rpt.OverallStatus(), fmt.Sprintf("validated %d tables at level %s", len(tables), opts.Level))

	if opts.ReportFile != "" {
		if err := rpt.Save(opts.ReportFile); err != nil {
			logger.Warn("failed to save report", zap.Error(err))
		}
	}

	return rpt, nil
}

func (v *Validator) validateRowCount(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, table string) reporter.TableReport {
	tr := reporter.TableReport{TableName: table, Status: reporter.StatusPass}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var sourceCount int64
	err := pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quotePG(schema), quotePG(table))).Scan(&sourceCount)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("source count: %v", err)
		return tr
	}

	var targetCount int64
	err = tidbConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQL(table))).Scan(&targetCount)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("target count: %v", err)
		return tr
	}

	tr.SourceRows = sourceCount
	tr.TargetRows = targetCount
	tr.DiffRows = sourceCount - targetCount

	if tr.DiffRows != 0 {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("row count mismatch: source=%d target=%d diff=%d", sourceCount, targetCount, tr.DiffRows)
	}

	return tr
}

func (v *Validator) validateSampling(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, tidbDB *sql.DB, table string, ratio float64) reporter.TableReport {
	tr := v.validateRowCount(ctx, pgDB, tidbConn, table)
	if tr.Status == reporter.StatusFail && tr.DiffRows != 0 {
		return tr
	}

	if tr.SourceRows == 0 {
		tr.Status = reporter.StatusPass
		return tr
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	// Detect table structure: does it have a primary key or unique index?
	keyInfo, err := v.detectTableKey(ctx, pgDB, schema, table)
	if err != nil {
		logger := zap.L()
		logger.Warn("failed to detect table key, assuming no PK", zap.String("table", table), zap.Error(err))
		keyInfo = &TableKeyInfo{} // treat as no-PK
	}
	needsNoPKStrategy := !keyInfo.HasPK && !keyInfo.HasUniqueIndex

	// For no-PK tables, decide which strategy to use
	if needsNoPKStrategy {
		strategy := v.cfg.Compare.NoPKStrategy
		if strategy == "" {
			strategy = "auto"
		}

		// Auto-select strategy based on table size
		if strategy == "auto" {
			threshold := v.cfg.Compare.NoPKTableThreshold
			if threshold <= 0 {
				threshold = 1000000
			}
			if tr.SourceRows <= threshold {
				strategy = "hash_group"
			} else {
				strategy = "aggregate" // Phase 2 will implement this
			}
		}

		if strategy == "hash_group" {
			return v.validateSamplingWithHashGroup(ctx, pgDB, tidbConn, table, ratio, tr, schema)
		}
		if strategy == "aggregate" {
			return v.validateNoPKWithAggregate(ctx, pgDB, tidbConn, table, tr, schema)
		}
		if strategy == "bucket" {
			return v.validateNoPKWithBucket(ctx, pgDB, tidbConn, table, tr, schema)
		}
		// Unknown strategy falls through to existing sampling logic
	}

	sampleSize := int(float64(tr.SourceRows) * ratio)
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > 1000 {
		sampleSize = 1000
	}

	offset := rand.Int63n(tr.SourceRows - int64(sampleSize) + 1)

	pgQuery := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY 1 LIMIT %d OFFSET %d",
		quotePG(schema), quotePG(table), sampleSize, offset)
	pgRows, err := pgDB.QueryContext(ctx, pgQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("sample source: %v", err)
		return tr
	}
	defer pgRows.Close()

	pgCols, _ := pgRows.ColumnTypes()
	if pgCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "failed to get PG column types"
		return tr
	}

	// Build sets of column indices to skip or trim in comparison.
	// Floating point types have inherent precision differences between PG and TiDB.
	// CHAR/VARCHAR/TEXT types may differ in trailing spaces (MySQL auto-trims CHAR).
	skipCols := make(map[int]bool)
	trimCols := make(map[int]bool)
	for i, c := range pgCols {
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) ||
				strings.Contains(dt, "json") {
			skipCols[i] = true
		}
		if dt == "character" || dt == "char" || dt == "bpchar" || dt == "character varying" || dt == "varchar" || dt == "text" {
			trimCols[i] = true
		}
	}

	pgValues := make([]interface{}, len(pgCols))
	pgPtrs := make([]interface{}, len(pgCols))
	for i := range pgValues {
		pgPtrs[i] = &pgValues[i]
	}

	var pgData [][]string
	for pgRows.Next() {
		if err := pgRows.Scan(pgPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("scan PG row: %v", err)
			return tr
		}
		row := make([]string, len(pgCols))
		for i, val := range pgValues {
			row[i] = normalizeValue(val)
		}
		pgData = append(pgData, row)
	}

// The Go code below should be inserted at the right indentation level.
	// Determine key columns for matching.
	// If the table has a PK (single or composite), use ALL PK columns as the key.
	var keyColIndices []int
	if keyInfo != nil && keyInfo.HasPK && len(keyInfo.PKColumns) > 0 {
		for _, pkCol := range keyInfo.PKColumns {
			for i, c := range pgCols {
				if strings.ToLower(c.Name()) == strings.ToLower(pkCol) {
					allNonNULL := true
					for _, row := range pgData {
						if i >= len(row) || row[i] == "\\N" {
							allNonNULL = false
							break
						}
					}
					if allNonNULL {
						keyColIndices = append(keyColIndices, i)
					}
					break
				}
			}
		}
	}

	// Fallback: if no PK or PK columns have NULLs, find first non-skipped column
	if len(keyColIndices) == 0 {
		for colIdx := 0; colIdx < len(pgCols); colIdx++ {
			if skipCols[colIdx] {
				continue
			}
			allNonNULL := true
			for _, row := range pgData {
				if colIdx >= len(row) || row[colIdx] == "\\N" {
					allNonNULL = false
					break
				}
			}
			if allNonNULL {
				keyColIndices = []int{colIdx}
				break
			}
		}
	}

	buildKey := func(row []string) string {
		var parts []string
		for _, idx := range keyColIndices {
			if idx < len(row) {
				parts = append(parts, row[idx])
			} else {
				parts = append(parts, "\\N")
			}
		}
		return strings.Join(parts, "|")
	}

	var mismatchCount int
	var mismatchDetails []string

	if len(keyColIndices) > 0 {
		isCompositePK := len(keyColIndices) > 1
		var tidbQuery string

		if isCompositePK {
			var colNames []string
			for _, idx := range keyColIndices {
				colNames = append(colNames, quoteMySQL(pgCols[idx].Name()))
			}
			seen := make(map[string]bool)
			var tupleParts []string
			for _, row := range pgData {
				key := buildKey(row)
				if seen[key] {
					continue
				}
				seen[key] = true
				var vals []string
				for _, idx := range keyColIndices {
					escaped := strings.ReplaceAll(row[idx], "'", "\\'")
					vals = append(vals, fmt.Sprintf("'%s'", escaped))
				}
				tupleParts = append(tupleParts, fmt.Sprintf("(%s)", strings.Join(vals, ",")))
			}
			if len(tupleParts) == 0 {
				tr.Status = reporter.StatusPass
				return tr
			}
			tidbQuery = fmt.Sprintf("SELECT * FROM %s WHERE (%s) IN (%s)",
				quoteMySQL(table), strings.Join(colNames, ","), strings.Join(tupleParts, ","))
		} else {
			keyColName := pgCols[keyColIndices[0]].Name()
			var whereParts []string
			seen := make(map[string]bool)
			for _, row := range pgData {
				val := row[keyColIndices[0]]
				if val == "\\N" || seen[val] {
					continue
				}
				seen[val] = true
				escaped := strings.ReplaceAll(val, "'", "\\'")
				whereParts = append(whereParts, fmt.Sprintf("'%s'", escaped))
			}
			if len(whereParts) == 0 {
				tr.Status = reporter.StatusPass
				return tr
			}
			tidbQuery = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
				quoteMySQL(table), quoteMySQL(keyColName), strings.Join(whereParts, ","))
		}

		tidbRows, err := tidbConn.QueryContext(ctx, tidbQuery)
		if err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("sample target: %v", err)
			return tr
		}
		defer tidbRows.Close()

		tidbCols, _ := tidbRows.ColumnTypes()
		if tidbCols == nil {
			tr.Status = reporter.StatusFail
			tr.Error = "failed to get TiDB column types"
			return tr
		}
		tidbValues := make([]interface{}, len(tidbCols))
		tidbPtrs := make([]interface{}, len(tidbCols))
		for i := range tidbValues {
			tidbPtrs[i] = &tidbValues[i]
		}

		// Build sorted column list for hash-based row comparison.
		// Hash-based comparison is more robust than per-column comparison
		// because it reuses the same normalizeValue + trim pipeline that
		// the hash_group and aggregate modes already use and test against.
		pgColNames := make([]string, len(pgCols))
		pgColNameToIdx := make(map[string]int)
		for i, c := range pgCols {
			pgColNames[i] = c.Name()
			pgColNameToIdx[strings.ToLower(c.Name())] = i
		}
		sortedPGColNames := make([]string, len(pgColNames))
		copy(sortedPGColNames, pgColNames)
		sort.Strings(sortedPGColNames)

		tidbColNameToIdx := make(map[string]int)
		for i, c := range tidbCols {
			tidbColNameToIdx[strings.ToLower(c.Name())] = i
		}

		// Build unified skip set (column name lowercase -> skip).
		// Skip a column if it should be skipped on EITHER side.
		unifiedSkipCols := make(map[string]bool)
		for _, name := range sortedPGColNames {
			lowerName := strings.ToLower(name)
			pgIdx, pgOk := pgColNameToIdx[lowerName]
			if pgOk && skipCols[pgIdx] {
				unifiedSkipCols[lowerName] = true
				continue
			}
			tidbIdx, tidbOk := tidbColNameToIdx[lowerName]
			if tidbOk {
				dt := strings.ToLower(tidbCols[tidbIdx].DatabaseTypeName())
				if isApproximateFloatType(dt) || strings.Contains(dt, "json") {
					unifiedSkipCols[lowerName] = true
					continue
				}
			}
		}

		// Build unified trim set (column name lowercase -> trim).
		trimColNames := make(map[string]bool)
		for _, name := range sortedPGColNames {
			lowerName := strings.ToLower(name)
			pgIdx, pgOk := pgColNameToIdx[lowerName]
			if pgOk {
				dt := strings.ToLower(pgCols[pgIdx].DatabaseTypeName())
				if isTextType(dt) {
					trimColNames[lowerName] = true
				}
			}
			tidbIdx, tidbOk := tidbColNameToIdx[lowerName]
			if tidbOk {
				dt := strings.ToLower(tidbCols[tidbIdx].DatabaseTypeName())
				if isTextType(dt) {
					trimColNames[lowerName] = true
				}
			}
		}

		// Build PG hash column mapping (sorted by name, skipping unified skips).
		var pgHashCols []colMapping
		for _, name := range sortedPGColNames {
			lowerName := strings.ToLower(name)
			if unifiedSkipCols[lowerName] {
				continue
			}
			idx := pgColNameToIdx[lowerName]
			if _, tidbOk := tidbColNameToIdx[lowerName]; !tidbOk {
				continue
			}
			pgHashCols = append(pgHashCols, colMapping{pgIdx: idx, name: name})
		}

		// Build TiDB hash column mapping using same unified set.
		var tidbHashCols []tidbColMapping
		for _, name := range sortedPGColNames {
			lowerName := strings.ToLower(name)
			if unifiedSkipCols[lowerName] {
				continue
			}
			idx, ok := tidbColNameToIdx[lowerName]
			if !ok {
				continue
			}
			tidbHashCols = append(tidbHashCols, tidbColMapping{tidbIdx: idx, name: name})
		}

		// Compute row hashes for PG sample data (hash -> list of keys for
		// disambiguation when multiple rows share the same hash).
		type pgRowEntry struct {
			hash string
			key  string
		}
		pgHashMap := make(map[string][]pgRowEntry) // hash -> entries
		pgKeyToIdx := make(map[string]int)         // key -> pgData index for diagnostics
		for rowIdx, row := range pgData {
			if len(keyColIndices) == 0 {
				continue
			}
			h := computeRowHashTrimmed(row, pgHashCols, trimColNames)
			key := buildKey(row)
			pgKeyToIdx[key] = rowIdx
			pgHashMap[h] = append(pgHashMap[h], pgRowEntry{hash: h, key: key})
		}

		// Build TiDB key builder for error reporting.
		tidbKeyColIndices := make(map[string]int) // PG col name (lower) -> TiDB col index
		for _, pgIdx := range keyColIndices {
			colName := strings.ToLower(pgCols[pgIdx].Name())
			for i, c := range tidbCols {
				if strings.ToLower(c.Name()) == colName {
					tidbKeyColIndices[colName] = i
					break
				}
			}
		}
		buildTiDBKeyFromRow := func(row []string) string {
			var parts []string
			for _, pgIdx := range keyColIndices {
				colName := strings.ToLower(pgCols[pgIdx].Name())
				if ti, ok := tidbKeyColIndices[colName]; ok && ti < len(row) {
					parts = append(parts, row[ti])
				} else {
					parts = append(parts, "\\N")
				}
			}
			return strings.Join(parts, "|")
		}
		for tidbRows.Next() {
			if err := tidbRows.Scan(tidbPtrs...); err != nil {
				continue
			}
			tidbRow := make([]string, len(tidbValues))
			for i, val := range tidbValues {
				tidbRow[i] = normalizeValue(val)
			}
			if len(tidbRow) == 0 {
				continue
			}

			h := computeTiDBRowHashTrimmed(tidbRow, tidbHashCols, trimColNames)
			entries, found := pgHashMap[h]
			if !found || len(entries) == 0 {
				key := buildTiDBKeyFromRow(tidbRow)
				mismatchCount++
				diag := ""
				if mismatchCount <= 3 {
					if pgIdx, ok := pgKeyToIdx[key]; ok {
						diag = diagnoseRowDiff(pgData[pgIdx], tidbRow, pgHashCols, tidbHashCols, trimColNames, pgCols, tidbCols)
					}
				}
				mismatchDetails = append(mismatchDetails, fmt.Sprintf("hash=%s not found in PG (key=%s)%s", truncate(h, 16), truncate(key, 40), diag))
				continue
			}
			// Remove first matching entry
			if len(entries) > 1 {
				pgHashMap[h] = entries[1:]
			} else {
				delete(pgHashMap, h)
			}
		}

		// Any remaining PG entries were not matched by TiDB.
		for _, entries := range pgHashMap {
			for _, e := range entries {
				mismatchCount++
				mismatchDetails = append(mismatchDetails, fmt.Sprintf("hash=%s in PG but not found in TiDB (key=%s)", truncate(e.hash, 16), truncate(e.key, 40)))
			}
		}

	} else {
		// Fallback for NULL first column: use positional comparison
		// (less reliable but necessary when key column is NULL)
		tidbQuery := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d",
			quoteMySQL(table), sampleSize, offset)
		tidbRows, err := tidbConn.QueryContext(ctx, tidbQuery)
		if err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("sample target: %v", err)
			return tr
		}
		defer tidbRows.Close()

		tidbCols, _ := tidbRows.ColumnTypes()
		tidbValues := make([]interface{}, len(tidbCols))
		tidbPtrs := make([]interface{}, len(tidbCols))
		for i := range tidbValues {
			tidbPtrs[i] = &tidbValues[i]
		}
		rowIdx := 0
		for tidbRows.Next() {
			if err := tidbRows.Scan(tidbPtrs...); err != nil {
				continue
			}
			if rowIdx < len(pgData) {
				for colIdx, val := range tidbValues {
					if skipCols[colIdx] {
						continue
					}
					pgVal := ""
					if colIdx < len(pgData[rowIdx]) {
						pgVal = pgData[rowIdx][colIdx]
					}
					tidbVal := normalizeValue(val)
					if trimCols[colIdx] {
						pgVal = trimTrailingWhitespace(pgVal)
						tidbVal = trimTrailingWhitespace(tidbVal)
					}
					if pgVal != tidbVal {
						mismatchCount++
						colName := tidbCols[colIdx].Name()
						mismatchDetails = append(mismatchDetails, fmt.Sprintf("row %d col %q: PG=%q TiDB=%q", rowIdx+int(offset)+1, colName, truncate(pgVal, 80), truncate(tidbVal, 80)))
						break
					}
				}
			}
			rowIdx++
		}
	}

	if mismatchCount > 0 {
		tr.Status = reporter.StatusFail
		maxShow := 10
		if len(mismatchDetails) > maxShow {
			mismatchDetails = mismatchDetails[:maxShow]
		}
		detailStr := strings.Join(mismatchDetails, "; ")
		tr.Error = fmt.Sprintf("%d/%d rows mismatch in sampling (%s)", mismatchCount, len(pgData), detailStr)
	} else {
		tr.Status = reporter.StatusPass
	}
	tr.Suggestion = fmt.Sprintf("sampled %d rows (%.1f%%), %d mismatches", len(pgData), ratio*100, mismatchCount)
	return tr

}

// validateSamplingWithHashGroup handles no-PK table validation using hash group
// comparison. It queries ALL rows from PG (hash_group is an exact strategy,
// not a sampled one), then uses validateHashGroup to compare the multiset of
// row hashes against TiDB's full table.
func (v *Validator) validateSamplingWithHashGroup(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, table string, ratio float64, tr reporter.TableReport, schema string) reporter.TableReport {
	logger := zap.L()

	// Hash group is an exact strategy — query the full PG table, not a sample.
	// Sampling would cause mismatches because TiDB is also queried in full.
	pgQuery := fmt.Sprintf("SELECT * FROM %s.%s",
		quotePG(schema), quotePG(table))
	pgRows, err := pgDB.QueryContext(ctx, pgQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("sample source (no-PK): %v", err)
		return tr
	}
	defer pgRows.Close()

	pgCols, _ := pgRows.ColumnTypes()
	if pgCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "failed to get PG column types"
		return tr
	}

	// Build skip/trim column sets (same logic as validateSampling)
	skipCols := make(map[int]bool)
	for i, c := range pgCols {
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) ||
			strings.Contains(dt, "json") {
			skipCols[i] = true
		}
	}

	pgValues := make([]interface{}, len(pgCols))
	pgPtrs := make([]interface{}, len(pgCols))
	for i := range pgValues {
		pgPtrs[i] = &pgValues[i]
	}

	var pgData [][]string
	for pgRows.Next() {
		if err := pgRows.Scan(pgPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("scan PG row: %v", err)
			return tr
		}
		row := make([]string, len(pgCols))
		for i, val := range pgValues {
			row[i] = normalizeValue(val)
		}
		pgData = append(pgData, row)
	}

	logger.Info("no-PK table: querying full PG table for hash group comparison",
		zap.String("table", table),
		zap.Int("row_count", len(pgData)))

	return v.validateHashGroup(ctx, pgDB, tidbConn, table, tr, pgCols, pgData, skipCols)
}

// validateNoPKWithAggregate wraps full-table PG query + aggregate hash validation.
func (v *Validator) validateNoPKWithAggregate(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, table string, tr reporter.TableReport, schema string) reporter.TableReport {
	pgQuery := fmt.Sprintf("SELECT * FROM %s.%s", quotePG(schema), quotePG(table))
	pgRows, err := pgDB.QueryContext(ctx, pgQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("aggregate hash: query PG: %v", err)
		return tr
	}
	defer pgRows.Close()

	pgCols, _ := pgRows.ColumnTypes()
	if pgCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "aggregate hash: failed to get PG column types"
		return tr
	}

	skipCols := make(map[int]bool)
	for i, c := range pgCols {
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) || strings.Contains(dt, "json") {
			skipCols[i] = true
		}
	}

	pgValues := make([]interface{}, len(pgCols))
	pgPtrs := make([]interface{}, len(pgCols))
	for i := range pgValues {
		pgPtrs[i] = &pgValues[i]
	}

	var pgData [][]string
	for pgRows.Next() {
		if err := pgRows.Scan(pgPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("aggregate hash: scan PG row: %v", err)
			return tr
		}
		row := make([]string, len(pgCols))
		for i, val := range pgValues {
			row[i] = normalizeValue(val)
		}
		pgData = append(pgData, row)
	}

	return v.validateAggregateHash(ctx, pgDB, tidbConn, table, tr, pgCols, pgData, skipCols)
}

// validateNoPKWithBucket wraps full-table PG query + bucket validation.
func (v *Validator) validateNoPKWithBucket(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, table string, tr reporter.TableReport, schema string) reporter.TableReport {
	pgQuery := fmt.Sprintf("SELECT * FROM %s.%s", quotePG(schema), quotePG(table))
	pgRows, err := pgDB.QueryContext(ctx, pgQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("bucket compare: query PG: %v", err)
		return tr
	}
	defer pgRows.Close()

	pgCols, _ := pgRows.ColumnTypes()
	if pgCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "bucket compare: failed to get PG column types"
		return tr
	}

	skipCols := make(map[int]bool)
	for i, c := range pgCols {
		dt := strings.ToLower(c.DatabaseTypeName())
		if isApproximateFloatType(dt) || strings.Contains(dt, "json") {
			skipCols[i] = true
		}
	}

	pgValues := make([]interface{}, len(pgCols))
	pgPtrs := make([]interface{}, len(pgCols))
	for i := range pgValues {
		pgPtrs[i] = &pgValues[i]
	}

	var pgData [][]string
	for pgRows.Next() {
		if err := pgRows.Scan(pgPtrs...); err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("bucket compare: scan PG row: %v", err)
			return tr
		}
		row := make([]string, len(pgCols))
		for i, val := range pgValues {
			row[i] = normalizeValue(val)
		}
		pgData = append(pgData, row)
	}

	return v.validateBucketCompare(ctx, pgDB, tidbConn, table, tr, pgCols, pgData, skipCols)
}

func (v *Validator) validateChecksum(ctx context.Context, pgDB *sql.DB, tidbConn *sql.Conn, table string) reporter.TableReport {
	tr := v.validateRowCount(ctx, pgDB, tidbConn, table)
	if tr.Status == reporter.StatusFail && tr.DiffRows != 0 {
		return tr
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	var pgChecksum sql.NullString
	err := pgDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT md5(string_agg(t::text, ',' ORDER BY id)) FROM (SELECT * FROM %s.%s ORDER BY 1) t",
			quotePG(schema), quotePG(table))).Scan(&pgChecksum)
	if err != nil {
		tr.Status = reporter.StatusWarn
		tr.Error = fmt.Sprintf("checksum source: %v", err)
		return tr
	}

	var tidbChecksum sql.NullString
	err = tidbConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MD5(GROUP_CONCAT(t ORDER BY id SEPARATOR ',')) FROM (SELECT * FROM %s ORDER BY 1) t",
			quoteMySQL(table))).Scan(&tidbChecksum)
	if err != nil {
		tr.Status = reporter.StatusWarn
		tr.Error = fmt.Sprintf("checksum target: %v", err)
		return tr
	}

	if pgChecksum.String != tidbChecksum.String {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("checksum mismatch: pg=%s tidb=%s", pgChecksum.String, tidbChecksum.String)
	}

	return tr
}

func (v *Validator) getTables(ctx context.Context, pgDB *sql.DB, include []string) ([]string, error) {
	if len(include) > 0 {
		return include, nil
	}

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := pgDB.QueryContext(ctx, query, schema)
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
		tables = append(tables, name)
	}
	return tables, nil
}

func quotePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func normalizeValue(val interface{}) string {
	if val == nil {
		return "\\N"
	}
	switch v := val.(type) {
	case bool:
		if v {
			return "1"
		}
		return "0"
	case float64:
		// Use strconv.FormatFloat with 'f' and -1 precision to preserve
		// full float64 precision (e.g., 123.4567 not "123").
		return normalizeString(strconv.FormatFloat(v, 'f', -1, 64))
	case float32:
		return normalizeString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case int64:
		return normalizeString(strconv.FormatInt(v, 10))
	case int:
		return normalizeString(strconv.Itoa(v))
	case []byte:
		return normalizeString(string(v))
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05")
	case string:
		return normalizeString(v)
	case fmt.Stringer:
		return normalizeString(v.String())
	default:
		return normalizeString(fmt.Sprintf("%v", v))
	}
}

var uuidRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
var pgArrayRe = regexp.MustCompile(`^\{.*\}$`)

// decimalRe matches numeric strings like "10.50", "-3.1400", "0.00"
var decimalRe = regexp.MustCompile(`^-?[0-9]+[.][0-9]+$`)

// normalizeDecimalString strips trailing zeros from decimal-looking strings.
// "10.50" -> "10.5", "10.00" -> "10", "10" -> "10" (unchanged, no decimal point).
func normalizeDecimalString(s string) string {
	if !decimalRe.MatchString(s) {
		return s
	}
	// Strip trailing zeros
	s = strings.TrimRight(s, "0")
	// Strip trailing decimal point if no fractional digits remain
	s = strings.TrimRight(s, ".")
	return s
}

// timestampRe matches common timestamp formats: "2006-01-02 15:04:05" with optional
// fractional seconds ".123456" and optional timezone "Z" or "+08:00".
var timestampRe = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\.\d+)?(.*)$`)

// normalizeTimestampString strips fractional seconds and normalizes timezone from
// timestamp-looking strings, so both PG and TiDB produce the same value.
func normalizeTimestampString(s string) string {
	m := timestampRe.FindStringSubmatch(s)
	if m == nil {
		return s
	}
	// Return just the base timestamp without fractional seconds or timezone suffix
	return m[1]
}

func normalizeString(s string) string {
	// Normalize line endings: \r\n → \n, then standalone \r → \n.
	// MySQL/TiDB may strip or normalize carriage returns differently than PG.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")

	// Normalize decimal numbers: strip trailing zeros so "10.50" and "10.5"
	// compare equal. This handles PG (string "10.50") vs TiDB (float64->"10.5").
	s = normalizeDecimalString(s)

	// Normalize timestamps: strip fractional seconds and timezone suffix
	// so "2024-01-01 12:30:00.000000" and "2024-01-01 12:30:00" compare equal.
	s = normalizeTimestampString(s)

	// Normalize UUID to lowercase
	s = uuidRe.ReplaceAllStringFunc(s, func(m string) string {
		return strings.ToLower(m)
	})
	// Normalize PG array format {1,2,3} -> [1,2,3] (must be before JSON check)
	if pgArrayRe.MatchString(s) {
		return normalizePGArray(s)
	}
	// Normalize JSON whitespace
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return normalizeJSON(s)
	}
	return s
}

func normalizePGArray(s string) string {
	// Convert PG array format {elem1,elem2,...} to JSON array ["elem1","elem2",...]
	// then normalize JSON whitespace for consistent comparison with TiDB JSON values.
	return normalizeJSON(pgArrayToJSON(s))
}

func pgArrayToJSON(s string) string {
	inner := s[1 : len(s)-1] // strip outer { }
	if inner == "" {
		return "[]"
	}
	elements := splitPGArrayElements(inner)
	parts := make([]string, 0, len(elements))
	for _, elem := range elements {
		elem = strings.TrimSpace(elem)
		if elem == "" || elem == "NULL" || elem == "null" {
			parts = append(parts, "null")
		} else if elem == "t" {
			parts = append(parts, "true")
		} else if elem == "f" {
			parts = append(parts, "false")
		} else if len(elem) >= 2 && elem[0] == '"' && elem[len(elem)-1] == '"' {
			// Already quoted in PG syntax — unescape PG "" → JSON \"
			unquoted := elem[1 : len(elem)-1]
			unquoted = strings.ReplaceAll(unquoted, `""`, `"`)
			b, _ := json.Marshal(unquoted)
			parts = append(parts, string(b))
		} else if len(elem) >= 2 && elem[0] == '{' && elem[len(elem)-1] == '}' {
			parts = append(parts, pgArrayToJSON(elem))
		} else {
			// Try number; otherwise treat as string and JSON-quote it
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

func normalizeJSON(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	inString := false
	escaped := false
	for _, r := range s {
		if escaped {
			buf.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && inString {
			buf.WriteRune(r)
			escaped = true
			continue
		}
		if r == '"' {
			inString = !inString
			buf.WriteRune(r)
			continue
		}
		if inString {
			buf.WriteRune(r)
			continue
		}
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		buf.WriteRune(r)
	}
	return buf.String()
}

// trimTrailingWhitespace removes trailing whitespace characters (space, tab,
// newline, carriage return) from a string. This is used for text-type column
// comparison because MySQL/TiDB may strip trailing whitespace differently than
// PostgreSQL (e.g., PG preserves \r\n while TiDB strips it).
func trimTrailingWhitespace(s string) string {
	return strings.TrimRight(s, " \t\n\r")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// diagnoseRowDiff compares a PG row and TiDB row column-by-column and returns
// a diagnostic string listing the first few column differences.
func diagnoseRowDiff(
	pgRow []string, tidbRow []string,
	pgHashCols []colMapping, tidbHashCols []tidbColMapping,
	trimColNames map[string]bool,
	pgCols []*sql.ColumnType, tidbCols []*sql.ColumnType,
) string {
	var diffs []string
	maxDiffs := 3
	for i := 0; i < len(pgHashCols) && i < len(tidbHashCols) && len(diffs) < maxDiffs; i++ {
		pgHC := pgHashCols[i]
		tidbHC := tidbHashCols[i]

		pgVal := "N"
		if pgHC.pgIdx < len(pgRow) {
			pgVal = pgRow[pgHC.pgIdx]
		}
		tidbVal := "N"
		if tidbHC.tidbIdx < len(tidbRow) {
			tidbVal = tidbRow[tidbHC.tidbIdx]
		}

		// Apply trim if this is a text column
		if trimColNames[strings.ToLower(pgHC.name)] {
			pgVal = trimTrailingWhitespace(pgVal)
			tidbVal = trimTrailingWhitespace(tidbVal)
		}

		if pgVal != tidbVal {
			pgType := "?"
			if pgHC.pgIdx < len(pgCols) {
				pgType = pgCols[pgHC.pgIdx].DatabaseTypeName()
			}
			tidbType := "?"
			if tidbHC.tidbIdx < len(tidbCols) {
				tidbType = tidbCols[tidbHC.tidbIdx].DatabaseTypeName()
			}
			diffs = append(diffs, fmt.Sprintf("%s PG(%s)=%q TiDB(%s)=%q", pgHC.name, pgType, truncate(pgVal, 60), tidbType, truncate(tidbVal, 60)))
		}
	}
	if len(diffs) == 0 {
		return " [hash-diff-but-all-cols-match?]"
	}
	return " diff=[" + strings.Join(diffs, "; ") + "]"
}
