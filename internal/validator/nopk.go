package validator

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

// TableKeyInfo holds information about a table's primary key and unique indexes.
type TableKeyInfo struct {
	HasPK          bool
	PKColumns      []string
	HasUniqueIndex bool
	UniqueColumns  []string // columns from the first unique index found
}

// colMapping maps a column name to its PG result-set index for hash computation.
type colMapping struct {
	pgIdx int
	name  string
}

// tidbColMapping maps a column name to its TiDB result-set index for hash computation.
type tidbColMapping struct {
	tidbIdx int
	name    string
}

// detectTableKey queries PG information_schema to determine whether a table
// has a primary key or unique index and returns the key columns.
func (v *Validator) detectTableKey(ctx context.Context, pgDB *sql.DB, schema, table string) (*TableKeyInfo, error) {
	info := &TableKeyInfo{}

	// Check for primary key
	pkRows, err := pgDB.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.table_schema = $1
			AND tc.table_name = $2
			AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query primary key: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pk column: %w", err)
		}
		info.PKColumns = append(info.PKColumns, col)
	}
	info.HasPK = len(info.PKColumns) > 0

	if info.HasPK {
		return info, nil
	}

	// No primary key — check for unique indexes
	// Query pg_indexes for unique indexes (not already covered by PK)
	uidxRows, err := pgDB.QueryContext(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = $1
			AND tablename = $2
			AND indexdef LIKE '%UNIQUE%'
			AND indexdef NOT LIKE '%pkey%'
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query unique indexes: %w", err)
	}
	defer uidxRows.Close()

	for uidxRows.Next() {
		var def string
		if err := uidxRows.Scan(&def); err != nil {
			return nil, fmt.Errorf("scan unique index def: %w", err)
		}
		// Parse column names from CREATE UNIQUE INDEX ... ON table (col1, col2, ...)
		cols := parseIndexColumns(def)
		if len(cols) > 0 {
			info.HasUniqueIndex = true
			info.UniqueColumns = cols
			break // use the first unique index found
		}
	}

	return info, nil
}

// parseIndexColumns extracts column names from a CREATE UNIQUE INDEX statement.
func parseIndexColumns(indexDef string) []string {
	// Find the last parenthesized group: ... ON table (col1, col2, ...)
	idx := strings.LastIndex(indexDef, "(")
	if idx < 0 {
		return nil
	}
	inner := indexDef[idx+1:]
	end := strings.Index(inner, ")")
	if end < 0 {
		return nil
	}
	inner = inner[:end]

	parts := strings.Split(inner, ",")
	var cols []string
	for _, p := range parts {
		col := strings.TrimSpace(p)
		// Remove optional ASC/DESC/NULLS options
		col = strings.Split(col, " ")[0]
		col = strings.Trim(col, "\"")
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

// validateHashGroup performs hash-group-based validation for tables without
// a reliable unique key. It computes a hash of each row's values (sorted by
// column name for cross-DB consistency) and compares the multiset of hashes
// between PG and TiDB.
func (v *Validator) validateHashGroup(ctx context.Context, pgDB, tidbDB *sql.DB, table string, tr reporter.TableReport, pgCols []*sql.ColumnType, pgData [][]string, skipCols map[int]bool) reporter.TableReport {
	logger := zap.L()
	logger.Info("using hash group validation for no-PK table", zap.String("table", table))

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	// Build ordered list of PG column names for consistent hashing.
	// Sort by column name so PG and TiDB hash the same column order.
	pgColNames := make([]string, len(pgCols))
	pgColNameToIdx := make(map[string]int)
	for i, c := range pgCols {
		pgColNames[i] = c.Name()
		pgColNameToIdx[strings.ToLower(c.Name())] = i
	}
	sortedPGColNames := make([]string, len(pgColNames))
	copy(sortedPGColNames, pgColNames)
	sort.Strings(sortedPGColNames)

	// Map sorted column names to PG column indices (excluding skipped columns)
	var hashCols []colMapping
	for _, name := range sortedPGColNames {
		idx := pgColNameToIdx[strings.ToLower(name)]
		if skipCols[idx] {
			continue
		}
		hashCols = append(hashCols, colMapping{pgIdx: idx, name: name})
	}

	// Compute row hashes for PG sample
	pgHashCounts := make(map[string]int) // hash -> count
	for _, row := range pgData {
		h := computeRowHash(row, hashCols)
		pgHashCounts[h]++
	}

	// Query TiDB for the full table (or same offset range as PG sample).
	// For hash group comparison, we need ALL TiDB rows, not just a sample,
	// because we compare multisets.
	tidbQuery := fmt.Sprintf("SELECT * FROM %s", quoteMySQL(table))
	tidbRows, err := tidbDB.QueryContext(ctx, tidbQuery)
	if err != nil {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("hash group: query TiDB: %v", err)
		return tr
	}
	defer tidbRows.Close()

	tidbCols, _ := tidbRows.ColumnTypes()
	if tidbCols == nil {
		tr.Status = reporter.StatusFail
		tr.Error = "hash group: failed to get TiDB column types"
		return tr
	}

	// Build TiDB column name -> index mapping
	tidbColNameToIdx := make(map[string]int)
	for i, c := range tidbCols {
		tidbColNameToIdx[strings.ToLower(c.Name())] = i
	}

	// Map sorted column names to TiDB column indices
	var tidbHashCols []tidbColMapping
	for _, name := range sortedPGColNames {
		idx, ok := tidbColNameToIdx[strings.ToLower(name)]
		if !ok {
			continue
		}
		// Check if this column is skipped in TiDB
		dt := strings.ToLower(tidbCols[idx].DatabaseTypeName())
		if isFloatType(dt) || strings.Contains(dt, "json") {
			continue
		}
		tidbHashCols = append(tidbHashCols, tidbColMapping{tidbIdx: idx, name: name})
	}

	tidbValues := make([]interface{}, len(tidbCols))
	tidbPtrs := make([]interface{}, len(tidbCols))
	for i := range tidbValues {
		tidbPtrs[i] = &tidbValues[i]
	}

	// Compute row hashes for TiDB
	tidbHashCounts := make(map[string]int)
	tidbRowCount := 0
	for tidbRows.Next() {
		if err := tidbRows.Scan(tidbPtrs...); err != nil {
			continue
		}
		tidbRow := make([]string, len(tidbValues))
		for i, val := range tidbValues {
			tidbRow[i] = normalizeValue(val)
		}
		h := computeTiDBRowHash(tidbRow, tidbHashCols)
		tidbHashCounts[h]++
		tidbRowCount++
	}

	// Compare multisets
	var mismatchDetails []string

	// Check PG hashes against TiDB
	for h, pgCnt := range pgHashCounts {
		tidbCnt := tidbHashCounts[h]
		if pgCnt != tidbCnt {
			if tidbCnt == 0 {
				mismatchDetails = append(mismatchDetails,
					fmt.Sprintf("PG has %d row(s) with hash %s not found in TiDB", pgCnt, truncate(h, 16)))
			} else {
				mismatchDetails = append(mismatchDetails,
					fmt.Sprintf("hash %s: PG count=%d TiDB count=%d", truncate(h, 16), pgCnt, tidbCnt))
			}
		}
	}

	// Check TiDB hashes not in PG
	for h, tidbCnt := range tidbHashCounts {
		pgCnt := pgHashCounts[h]
		if pgCnt == 0 {
			mismatchDetails = append(mismatchDetails,
				fmt.Sprintf("TiDB has %d row(s) with hash %s not found in PG", tidbCnt, truncate(h, 16)))
		}
	}

	if len(mismatchDetails) > 0 {
		tr.Status = reporter.StatusFail
		maxShow := 10
		if len(mismatchDetails) > maxShow {
			mismatchDetails = mismatchDetails[:maxShow]
		}
		tr.Error = fmt.Sprintf("hash group mismatch: %s", strings.Join(mismatchDetails, "; "))
	} else {
		tr.Status = reporter.StatusPass
	}

	tr.Suggestion = fmt.Sprintf("hash group validation: %d PG hashes vs %d TiDB rows, %d mismatches",
		len(pgHashCounts), tidbRowCount, len(mismatchDetails))

	return tr
}

// computeRowHash computes MD5 of a PG row's values, using only the columns
// specified in hashCols, joined by "|" in sorted column name order.
func computeRowHash(row []string, hashCols []colMapping) string {
	var buf strings.Builder
	for i, hc := range hashCols {
		if i > 0 {
			buf.WriteByte('|')
		}
		val := "\\N"
		if hc.pgIdx < len(row) {
			val = row[hc.pgIdx]
		}
		buf.WriteString(val)
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(buf.String())))
}

// computeTiDBRowHash computes MD5 of a TiDB row's values, using only the
// columns specified in hashCols, joined by "|" in sorted column name order.
func computeTiDBRowHash(row []string, hashCols []tidbColMapping) string {
	var buf strings.Builder
	for i, hc := range hashCols {
		if i > 0 {
			buf.WriteByte('|')
		}
		val := "\\N"
		if hc.tidbIdx < len(row) {
			val = row[hc.tidbIdx]
		}
		buf.WriteString(val)
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(buf.String())))
}

// isFloatType checks if a database type name is a floating-point type.
func isFloatType(dt string) bool {
	return dt == "real" || dt == "float" || dt == "float4" || dt == "float8" ||
		dt == "double" || dt == "double precision" || dt == "numeric" || dt == "decimal"
}
