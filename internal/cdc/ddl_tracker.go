package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// DDLTracker monitors PG DDL changes and transforms them for TiDB.
// It uses PG's event trigger facility to capture DDL statements.
type DDLTracker struct {
	db  *sql.DB
	log *zap.Logger

	mu        sync.Mutex
	ddlLog    []DDLEntry
	filter    *TableFilter
	transform *DDLTransformer
}

// DDLEntry records a captured DDL statement.
type DDLEntry struct {
	ID         int64  `json:"id"` // pg2tidb_ddl_log.id (DDL checkpoint cursor, #t59)
	LSN        string `json:"lsn"`
	Schema     string `json:"schema"`
	ObjectName string `json:"object_name"`
	ObjectType string `json:"object_type"` // TABLE, INDEX, VIEW, FUNCTION, etc.
	DDL        string `json:"ddl"`
	TiDBDDL    string `json:"tidb_ddl,omitempty"`
}

// DDLTransformer converts PG DDL statements to TiDB-compatible DDL.
type DDLTransformer struct{}

// NewDDLTransformer creates a new DDL transformer.
func NewDDLTransformer() *DDLTransformer {
	return &DDLTransformer{}
}

// NewDDLTracker creates a new DDL tracker.
func NewDDLTracker(db *sql.DB, filter *TableFilter) *DDLTracker {
	if filter == nil {
		filter = NewTableFilter()
	}
	return &DDLTracker{
		db:        db,
		log:       zap.NewNop(),
		filter:    filter,
		transform: NewDDLTransformer(),
	}
}

// SetLogger sets the logger.
func (t *DDLTracker) SetLogger(log *zap.Logger) {
	t.log = log
}

// SetupEventTrigger creates the PG event trigger function and trigger
// to capture DDL changes. Call this once during initialization.
func (t *DDLTracker) SetupEventTrigger(ctx context.Context) error {
	// Create the event trigger function
	_, err := t.db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION pg2tidb_ddl_capture()
		RETURNS event_trigger
		LANGUAGE plpgsql AS $$
		DECLARE
			r RECORD;
		BEGIN
			FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
			LOOP
				INSERT INTO pg2tidb_ddl_log (ddl_time, schema_name, object_name,
					object_type, ddl_command, txid)
				VALUES (now(), r.schema_name, r.object_identity,
					r.object_type, current_query(), txid_current());
			END LOOP;
		END;
		$$;
	`)
	if err != nil {
		return fmt.Errorf("create event trigger function: %w", err)
	}

	// Create the DDL log table
	_, err = t.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pg2tidb_ddl_log (
			id SERIAL PRIMARY KEY,
			ddl_time TIMESTAMPTZ DEFAULT now(),
			schema_name TEXT,
			object_name TEXT,
			object_type TEXT,
			ddl_command TEXT,
			txid BIGINT,
			lsn_txid BIGINT
		);
	`)
	if err != nil {
		return fmt.Errorf("create ddl log table: %w", err)
	}

	// Create the event trigger
	_, err = t.db.ExecContext(ctx, `
		DROP EVENT TRIGGER IF EXISTS pg2tidb_ddl_trigger;
		CREATE EVENT TRIGGER pg2tidb_ddl_trigger
		ON ddl_command_end
		EXECUTE FUNCTION pg2tidb_ddl_capture();
	`)
	if err != nil {
		return fmt.Errorf("create event trigger: %w", err)
	}

	t.log.Info("ddl tracker: event trigger setup complete")
	return nil
}

// TeardownEventTrigger removes the event trigger and log table.
func (t *DDLTracker) TeardownEventTrigger(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, `DROP EVENT TRIGGER IF EXISTS pg2tidb_ddl_trigger;`)
	if err != nil {
		t.log.Warn("drop event trigger", zap.Error(err))
	}
	_, err = t.db.ExecContext(ctx, `DROP FUNCTION IF EXISTS pg2tidb_ddl_capture();`)
	if err != nil {
		t.log.Warn("drop event trigger function", zap.Error(err))
	}
	return nil
}

// FetchNewDDL queries the DDL log for entries since the last checkpoint.
func (t *DDLTracker) FetchNewDDL(ctx context.Context, sinceID int64) ([]DDLEntry, error) {
	rows, err := t.db.QueryContext(ctx, `
		SELECT id, ddl_time, schema_name, object_name, object_type, ddl_command
		FROM pg2tidb_ddl_log
		WHERE id > $1
		ORDER BY id ASC
	`, sinceID)
	if err != nil {
		return nil, fmt.Errorf("fetch ddl log: %w", err)
	}
	defer rows.Close()

	var entries []DDLEntry
	for rows.Next() {
		var id int64
		var e DDLEntry
		if err := rows.Scan(&id, &e.LSN, &e.Schema, &e.ObjectName,
			&e.ObjectType, &e.DDL); err != nil {
			return nil, fmt.Errorf("scan ddl entry: %w", err)
		}
		e.ID = id
		e.LSN = fmt.Sprintf("ddl_%d", id)

		// Transform DDL for TiDB
		if t.filter.Allow(e.Schema, e.ObjectName) {
			e.TiDBDDL = t.transform.Transform(e.DDL, e.ObjectType)
			entries = append(entries, e)
		}
	}

	t.mu.Lock()
	t.ddlLog = append(t.ddlLog, entries...)
	if len(t.ddlLog) > 1000 {
		t.ddlLog = t.ddlLog[len(t.ddlLog)-1000:]
	}
	t.mu.Unlock()

	return entries, nil
}

// RecentDDL returns the last N DDL entries.
func (t *DDLTracker) RecentDDL(n int) []DDLEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n <= 0 || n > len(t.ddlLog) {
		n = len(t.ddlLog)
	}
	return t.ddlLog[len(t.ddlLog)-n:]
}

// Transform converts a PG DDL statement to TiDB-compatible DDL.
func (dt *DDLTransformer) Transform(ddl string, objectType string) string {
	ddl = strings.TrimSpace(ddl)

	switch strings.ToUpper(objectType) {
	case "TABLE":
		return dt.transformTableDDL(ddl)
	case "INDEX":
		return dt.transformIndexDDL(ddl)
	case "VIEW":
		return dt.transformViewDDL(ddl)
	case "FUNCTION":
		return dt.transformFunctionDDL(ddl)
	case "TRIGGER":
		return dt.transformTriggerDDL(ddl)
	default:
		return "-- TODO: transform " + objectType + "\n" + ddl
	}
}

func (dt *DDLTransformer) transformTableDDL(ddl string) string {
	// Cheap at-least-once idempotency (#t59 §4.2): CREATE/DROP get IF NOT
	// EXISTS / IF EXISTS so a replayed DDL doesn't error. ALTER is not
	// idempotent and relies on the ddl_log.id checkpoint to avoid replay
	// (a crash-window replay that errors → halt, not silently masked).
	ddl = makeDDLIdempotent(ddl)
	upper := strings.ToUpper(ddl)

	// Replace PG-specific types
	ddl = strings.ReplaceAll(ddl, "SERIAL", "BIGINT AUTO_INCREMENT")
	ddl = strings.ReplaceAll(ddl, "BIGSERIAL", "BIGINT AUTO_INCREMENT")
	ddl = strings.ReplaceAll(ddl, "SMALLSERIAL", "INT AUTO_INCREMENT")
	ddl = strings.ReplaceAll(ddl, "TEXT[]", "JSON")
	ddl = strings.ReplaceAll(ddl, "INTEGER[]", "JSON")
	ddl = strings.ReplaceAll(ddl, "VARCHAR[]", "JSON")
	ddl = strings.ReplaceAll(ddl, "BYTEA", "BLOB")
	ddl = strings.ReplaceAll(ddl, "TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
	ddl = strings.ReplaceAll(ddl, "TIMESTAMP WITHOUT TIME ZONE", "DATETIME")
	ddl = strings.ReplaceAll(ddl, "TIMESTAMPTZ", "TIMESTAMP")
	ddl = strings.ReplaceAll(ddl, "BOOLEAN", "BOOLEAN") // same in both
	ddl = strings.ReplaceAll(ddl, "JSONB", "JSON")
	ddl = strings.ReplaceAll(ddl, "UUID", "CHAR(36)")
	ddl = strings.ReplaceAll(ddl, "MONEY", "DECIMAL(19,2)")

	// Replace PG-specific syntax
	ddl = strings.ReplaceAll(ddl, "IF NOT EXISTS", "IF NOT EXISTS")         // same
	ddl = strings.ReplaceAll(ddl, "ON DELETE CASCADE", "ON DELETE CASCADE") // same

	// Replace PG-only USING clauses in index creation
	if strings.Contains(upper, "USING BTREE") {
		ddl = strings.ReplaceAll(ddl, "USING BTREE", "")
		ddl = strings.ReplaceAll(ddl, "USING btree", "")
	}
	if strings.Contains(upper, "USING HASH") {
		ddl = strings.ReplaceAll(ddl, "USING HASH", "")
		ddl = strings.ReplaceAll(ddl, "USING hash", "")
	}

	return ddl
}

func (dt *DDLTransformer) transformIndexDDL(ddl string) string {
	upper := strings.ToUpper(ddl)

	// Remove PG-only index methods
	if strings.Contains(upper, "USING GIN") || strings.Contains(upper, "USING gin") {
		return "-- GIN index not supported in TiDB, consider JSON index alternative:\n-- " + ddl
	}
	if strings.Contains(upper, "USING GIST") || strings.Contains(upper, "USING gist") {
		return "-- GiST index not supported in TiDB:\n-- " + ddl
	}
	if strings.Contains(upper, "USING BRIN") || strings.Contains(upper, "USING brin") {
		return "-- BRIN index not supported in TiDB, use partitioning instead:\n-- " + ddl
	}

	ddl = strings.ReplaceAll(ddl, "USING BTREE", "")
	ddl = strings.ReplaceAll(ddl, "USING btree", "")
	ddl = strings.ReplaceAll(ddl, "USING HASH", "")
	ddl = strings.ReplaceAll(ddl, "USING hash", "")

	// Partial index → comment out
	if strings.Contains(upper, " WHERE ") || strings.Contains(upper, " WHERE\n") {
		return "-- Partial index not supported in TiDB:\n-- " + ddl
	}

	return ddl
}

func (dt *DDLTransformer) transformViewDDL(ddl string) string {
	// Views need manual review; return commented-out
	return "-- View needs manual review for TiDB compatibility:\n-- " + ddl
}

func (dt *DDLTransformer) transformFunctionDDL(ddl string) string {
	return "-- Functions not supported in TiDB, use application logic:\n-- " + ddl
}

func (dt *DDLTransformer) transformTriggerDDL(ddl string) string {
	// Basic trigger transformation: plpgsql → application logic comment
	return "-- Trigger needs conversion to application logic for TiDB:\n-- " + ddl
}

// makeDDLIdempotent rewrites a CREATE/DROP TABLE statement to be safely
// replayable (IF NOT EXISTS / IF EXISTS) for at-least-once DDL apply (#t59 §4.2).
// Statements already carrying the guard, and non-CREATE/DROP DDL (ALTER), are
// returned unchanged — ALTER is not cheaply idempotent and relies on the
// ddl_log.id checkpoint to avoid replay.
func makeDDLIdempotent(ddl string) string {
	upper := strings.ToUpper(ddl)
	if idx := strings.Index(upper, "CREATE TABLE"); idx >= 0 && !strings.Contains(upper, "IF NOT EXISTS") {
		return ddl[:idx] + "CREATE TABLE IF NOT EXISTS" + ddl[idx+len("CREATE TABLE"):]
	}
	if idx := strings.Index(upper, "DROP TABLE"); idx >= 0 && !strings.Contains(upper, "IF EXISTS") {
		return ddl[:idx] + "DROP TABLE IF EXISTS" + ddl[idx+len("DROP TABLE"):]
	}
	return ddl
}
