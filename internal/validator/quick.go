package validator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pg2tidb/pg2tidb-migrator/internal/common/reporter"
	"go.uber.org/zap"
)

// validateQuick performs fast row count estimation using pg_stat_user_tables
// for PG and SHOW TABLE STATUS for TiDB, avoiding full table scans.
func (v *Validator) validateQuick(ctx context.Context, pgDB, tidbDB *sql.DB, table string) reporter.TableReport {
	tr := reporter.TableReport{TableName: table, Status: reporter.StatusPass}
	logger := zap.L()

	schema := v.cfg.Source.Schema
	if schema == "" {
		schema = "public"
	}

	// PG: use pg_stat_user_tables for fast row count estimation
	var pgCount sql.NullInt64
	err := pgDB.QueryRowContext(ctx, `
		SELECT COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = $2
	`, schema, table).Scan(&pgCount)
	if err != nil {
		logger.Warn("quick mode: pg_stat estimate failed, falling back to COUNT(*)",
			zap.String("table", table), zap.Error(err))
		// Fallback to exact COUNT(*)
		err = pgDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quotePG(schema), quotePG(table))).Scan(&pgCount)
		if err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("quick: PG count: %v", err)
			return tr
		}
	}

	// TiDB: use SHOW TABLE STATUS for fast row count estimation
	var tidbCount sql.NullInt64
	var tidbName, tidbEngine, tidbVersion sql.NullString
	var tidbRowFormat, tidbRows, tidbAvgRowLen, tidbDataLen, tidbMaxDataLen, tidbIndexLen, tidbAutoInc, tidbCreateTime, tidbUpdateTime, tidbCheckTime, tidbCollation, tidbChecksum, tidbCreateOpts, tidbComment sql.NullString
	err = tidbDB.QueryRowContext(ctx,
		fmt.Sprintf("SHOW TABLE STATUS LIKE '%s'", escapeSQLLike(table))).Scan(
		&tidbName, &tidbEngine, &tidbVersion, &tidbRowFormat, &tidbRows,
		&tidbAvgRowLen, &tidbDataLen, &tidbMaxDataLen, &tidbIndexLen,
		&tidbAutoInc, &tidbCreateTime, &tidbUpdateTime, &tidbCheckTime,
		&tidbCollation, &tidbChecksum, &tidbCreateOpts, &tidbComment)
	if err != nil {
		logger.Warn("quick mode: SHOW TABLE STATUS failed, falling back to COUNT(*)",
			zap.String("table", table), zap.Error(err))
		// Fallback to exact COUNT(*)
		err = tidbDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMySQL(table))).Scan(&tidbCount)
		if err != nil {
			tr.Status = reporter.StatusFail
			tr.Error = fmt.Sprintf("quick: TiDB count: %v", err)
			return tr
		}
	} else {
		// Parse the Rows field from SHOW TABLE STATUS (NullString -> NullInt64)
		if tidbRows.Valid {
			var val int64
			if _, err := fmt.Sscanf(tidbRows.String, "%d", &val); err == nil {
				tidbCount = sql.NullInt64{Int64: val, Valid: true}
			}
		}
	}

	sourceCount := pgCount.Int64
	targetCount := tidbCount.Int64
	tr.SourceRows = sourceCount
	tr.TargetRows = targetCount
	tr.DiffRows = sourceCount - targetCount

	if tr.DiffRows != 0 {
		tr.Status = reporter.StatusFail
		tr.Error = fmt.Sprintf("row count mismatch (estimated): source=%d target=%d diff=%d", sourceCount, targetCount, tr.DiffRows)
	}

	tr.Suggestion = "quick mode: row count estimation (no full scan)"
	return tr
}

// escapeSQLLike escapes special characters in a SQL LIKE pattern.
func escapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}
