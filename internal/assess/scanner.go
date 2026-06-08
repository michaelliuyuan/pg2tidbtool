package assess

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Scanner reads schema information from a PostgreSQL database.
type Scanner struct {
	db     *sql.DB
	schema string
}

// NewScanner creates a new Scanner.
func NewScanner(db *sql.DB, schema string) *Scanner {
	if schema == "" {
		schema = "public"
	}
	return &Scanner{db: db, schema: schema}
}

// ScanAll scans all schema objects from PostgreSQL.
func (s *Scanner) ScanAll(ctx context.Context) (*ScanResult, error) {
	result := &ScanResult{}

	tables, err := s.scanTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan tables: %w", err)
	}
	result.Tables = tables

	columns, err := s.scanColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan columns: %w", err)
	}
	result.Columns = columns

	indexes, err := s.scanIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan indexes: %w", err)
	}
	result.Indexes = indexes

	views, err := s.scanViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan views: %w", err)
	}
	result.Views = views

	functions, err := s.scanFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan functions: %w", err)
	}
	result.Functions = functions

	triggers, err := s.scanTriggers(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan triggers: %w", err)
	}
	result.Triggers = triggers

	enums, err := s.scanEnums(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan enums: %w", err)
	}
	result.Enums = enums

	extensions, err := s.scanExtensions(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan extensions: %w", err)
	}
	result.Extensions = extensions

	sequences, err := s.scanSequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan sequences: %w", err)
	}
	result.Sequences = sequences

	return result, nil
}

func (s *Scanner) scanTables(ctx context.Context) ([]TableInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}

func (s *Scanner) scanColumns(ctx context.Context) ([]ColumnInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.table_schema,
			c.table_name,
			c.column_name,
			c.data_type,
			COALESCE(c.character_maximum_length, 0),
			COALESCE(c.numeric_precision, 0),
			COALESCE(c.numeric_scale, 0),
			c.is_nullable = 'YES',
			COALESCE(c.column_default, ''),
			COALESCE((SELECT COUNT(*) > 0
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON tc.constraint_name = kcu.constraint_name
				WHERE tc.table_schema = c.table_schema
					AND tc.table_name = c.table_name
					AND tc.constraint_type = 'PRIMARY KEY'
					AND kcu.column_name = c.column_name
			), false),
			c.ordinal_position
		FROM information_schema.columns c
		WHERE c.table_schema = $1
		ORDER BY c.table_name, c.ordinal_position
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName,
			&c.DataType, &c.MaxLength, &c.NumericPrec, &c.NumericScale,
			&c.IsNullable, &c.ColumnDefault, &c.IsPrimary, &c.OrdinalPosition); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	return columns, nil
}

func (s *Scanner) scanIndexes(ctx context.Context) ([]IndexInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			i.tablename,
			i.indexname,
			COALESCE(
				CASE WHEN i.indexdef LIKE '%USING btree%' THEN 'btree'
					 WHEN i.indexdef LIKE '%USING hash%' THEN 'hash'
					 WHEN i.indexdef LIKE '%USING gin%' THEN 'gin'
					 WHEN i.indexdef LIKE '%USING gist%' THEN 'gist'
					 WHEN i.indexdef LIKE '%USING brin%' THEN 'brin'
					 WHEN i.indexdef LIKE '%USING spgist%' THEN 'spgist'
					 ELSE 'btree'
				END, 'btree'
			),
			i.indexdef LIKE '%UNIQUE%' OR i.indexdef LIKE '%PRIMARY KEY%' OR i.indexname LIKE '%pkey',
			i.indexdef LIKE '%PRIMARY KEY%' OR i.indexname LIKE '%pkey',
			i.indexdef,
			CASE WHEN i.indexdef LIKE '%WHERE%' THEN true ELSE false END,
			CASE WHEN i.indexdef ~ '\([^)]*\)' AND i.indexdef NOT LIKE '%USING %(%.%)' THEN true ELSE false END
		FROM pg_indexes i
		WHERE i.schemaname = $1
		ORDER BY i.tablename, i.indexname
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		if err := rows.Scan(&idx.TableName, &idx.Name, &idx.IndexType,
			&idx.IsUnique, &idx.IsPrimary, &idx.Definition,
			&idx.IsPartial, &idx.IsExpression); err != nil {
			return nil, err
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func (s *Scanner) scanViews(ctx context.Context) ([]ViewInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			v.table_schema,
			v.table_name,
			v.view_definition,
			COALESCE(pg_get_viewdef(c.oid, true), '')
		FROM information_schema.views v
		JOIN pg_class c ON c.relname = v.table_name
		JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = v.table_schema
		WHERE v.table_schema = $1
		ORDER BY v.table_name
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []ViewInfo
	for rows.Next() {
		var v ViewInfo
		if err := rows.Scan(&v.Schema, &v.Name, &v.Definition, &v.DDL); err != nil {
			return nil, err
		}
		v.Definition = strings.TrimSpace(v.Definition)
		views = append(views, v)
	}
	return views, nil
}

func (s *Scanner) scanFunctions(ctx context.Context) ([]FunctionInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			n.nspname,
			p.proname,
			COALESCE(pg_get_function_result(p.oid), 'void'),
			CASE WHEN l.lanname = 'plpgsql' THEN 'plpgsql'
				  WHEN l.lanname = 'sql' THEN 'sql'
				  ELSE l.lanname::text END,
			p.prosrc,
			p.prokind = 'p',
			COALESCE(pg_get_functiondef(p.oid), '')
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		WHERE n.nspname = $1
			AND l.lanname IN ('plpgsql', 'sql')
		ORDER BY p.proname
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []FunctionInfo
	for rows.Next() {
		var f FunctionInfo
		if err := rows.Scan(&f.Schema, &f.Name, &f.ReturnType,
			&f.Language, &f.Source, &f.IsProcedure, &f.DDL); err != nil {
			return nil, err
		}
		functions = append(functions, f)
	}
	return functions, nil
}

func (s *Scanner) scanTriggers(ctx context.Context) ([]TriggerInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			c.relname,
			t.tgname,
			CASE t.tgtype & 28
				WHEN 4 THEN 'INSERT'
				WHEN 8 THEN 'DELETE'
				WHEN 12 THEN 'INSERT, DELETE'
				WHEN 16 THEN 'UPDATE'
				WHEN 20 THEN 'INSERT, UPDATE'
				WHEN 24 THEN 'UPDATE, DELETE'
				WHEN 28 THEN 'INSERT, UPDATE, DELETE'
				ELSE 'UNKNOWN'
			END,
			CASE WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
				  WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
				  ELSE 'AFTER'
			END,
			p.prosrc,
			pg_get_triggerdef(t.oid)
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_proc p ON t.tgfoid = p.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
			AND NOT t.tgisinternal
		ORDER BY c.relname, t.tgname
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []TriggerInfo
	for rows.Next() {
		var t TriggerInfo
		if err := rows.Scan(&t.TableName, &t.Name, &t.EventType,
			&t.Timing, &t.Statement, &t.DDL); err != nil {
			return nil, err
		}
		triggers = append(triggers, t)
	}
	return triggers, nil
}

func (s *Scanner) scanEnums(ctx context.Context) ([]EnumInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			n.nspname,
			t.typname,
			array_agg(e.enumlabel ORDER BY e.enumsortorder)
		FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		JOIN pg_enum e ON t.oid = e.enumtypid
		WHERE n.nspname = $1
		GROUP BY n.nspname, t.typname
		ORDER BY t.typname
	`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enums []EnumInfo
	for rows.Next() {
		var e EnumInfo
		var vals string // PG array literal {val1,val2,...}
		if err := rows.Scan(&e.Schema, &e.Name, &vals); err != nil {
			return nil, err
		}
		e.Values = parsePGArray(vals)
		enums = append(enums, e)
	}
	return enums, nil
}

func (s *Scanner) scanExtensions(ctx context.Context) ([]ExtensionInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT extname, extversion, true
		FROM pg_extension
		ORDER BY extname
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var extensions []ExtensionInfo
	for rows.Next() {
		var e ExtensionInfo
		if err := rows.Scan(&e.Name, &e.Version, &e.Installed); err != nil {
			return nil, err
		}
		extensions = append(extensions, e)
	}
	return extensions, nil
}

func (s *Scanner) scanSequences(ctx context.Context) ([]SequenceInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			n.nspname,
			c.relname,
			'bigint',
			COALESCE(s.start_value::bigint, 1),
			COALESCE(s.increment_by::bigint, 1),
			COALESCE(s.max_value::bigint, 9223372036854775807),
			COALESCE(s.min_value::bigint, 1)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_sequences s ON c.relname = s.sequencename AND n.nspname = s.schemaname
		WHERE n.nspname = $1 AND c.relkind = 'S'
		ORDER BY c.relname
	`, s.schema)
	if err != nil {
		// Fallback: simpler query without pg_sequences
		rows2, err2 := s.db.QueryContext(ctx, `
			SELECT
				n.nspname,
				c.relname,
				'bigint',
				1,
				1,
				9223372036854775807,
				1
			FROM pg_class c
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE n.nspname = $1 AND c.relkind = 'S'
			ORDER BY c.relname
		`, s.schema)
		if err2 != nil {
			return nil, err
		}
		defer rows2.Close()
		return scanSequenceRows(rows2)
	}
	defer rows.Close()
	return scanSequenceRows(rows)
}

func scanSequenceRows(rows *sql.Rows) ([]SequenceInfo, error) {
	var seqs []SequenceInfo
	for rows.Next() {
		var seq SequenceInfo
		if err := rows.Scan(&seq.Schema, &seq.Name, &seq.DataType,
			&seq.StartValue, &seq.Increment, &seq.MaxValue, &seq.MinValue); err != nil {
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	return seqs, nil
}

// parsePGArray converts a PG array literal "{a,b,c}" to []string.
func parsePGArray(s string) []string {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return nil
	}
	return strings.Split(inner, ",")
}
