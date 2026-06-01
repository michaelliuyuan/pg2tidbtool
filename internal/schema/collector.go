package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type Collector struct {
	db *sql.DB
}

func NewCollector(db *sql.DB) *Collector {
	return &Collector{db: db}
}

func (c *Collector) CollectTables(ctx context.Context, schema string, excludeTables []string) ([]TableInfo, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := c.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !contains(excludeTables, name) {
			tableNames = append(tableNames, name)
		}
	}

	var tables []TableInfo
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)

	for _, name := range tableNames {
		wg.Add(1)
		sem <- struct{}{}
		go func(tableName string) {
			defer wg.Done()
			defer func() { <-sem }()

			table, err := c.collectTable(ctx, schema, tableName)
			if err != nil {
				zap.L().Warn("failed to collect table", zap.String("table", tableName), zap.Error(err))
				return
			}
			mu.Lock()
			tables = append(tables, *table)
			mu.Unlock()
		}(name)
	}

	wg.Wait()
	return tables, nil
}

func (c *Collector) collectTable(ctx context.Context, schema, tableName string) (*TableInfo, error) {
	t := &TableInfo{Schema: schema, Name: tableName}

	cols, err := c.collectColumns(ctx, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("collect columns for %s: %w", tableName, err)
	}
	t.Columns = cols

	indexes, err := c.collectIndexes(ctx, schema, tableName)
	if err != nil {
		zap.L().Warn("failed to collect indexes", zap.String("table", tableName), zap.Error(err))
	}
	t.Indexes = indexes

	fks, err := c.collectForeignKeys(ctx, schema, tableName)
	if err != nil {
		zap.L().Warn("failed to collect foreign keys", zap.String("table", tableName), zap.Error(err))
	}
	t.ForeignKeys = fks

	comment, err := c.collectTableComment(ctx, schema, tableName)
	if err == nil {
		t.Comment = comment
	}

	return t, nil
}

func (c *Collector) collectColumns(ctx context.Context, schema, table string) ([]Column, error) {
	query := `
		SELECT
			c.column_name,
			c.ordinal_position,
			c.data_type,
			c.udt_name,
			COALESCE(c.character_maximum_length, 0),
			COALESCE(c.numeric_precision, 0),
			COALESCE(c.numeric_scale, 0),
			c.is_nullable = 'YES',
			COALESCE(c.column_default, ''),
			COALESCE(col_description(pgc.oid, c.ordinal_position), '')
		FROM information_schema.columns c
		JOIN pg_class pgc ON pgc.relname = c.table_name
		JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`
	rows, err := c.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var dataType, udtName string
		if err := rows.Scan(
			&col.ColumnName, &col.OrdinalPos, &dataType, &udtName,
			&col.MaxLength, &col.NumericPrec, &col.NumericScale,
			&col.IsNullable, &col.DefaultValue, &col.Comment,
		); err != nil {
			return nil, err
		}
		col.TableName = table
		col.DataType = dataType
		col.PGType = resolvePGType(dataType, udtName)
		col.IsAutoIncr = strings.Contains(strings.ToUpper(col.DefaultValue), "NEXTVAL")
		columns = append(columns, col)
	}
	return columns, nil
}

func (c *Collector) collectIndexes(ctx context.Context, schema, table string) ([]Index, error) {
	query := `
		SELECT
			i.relname AS index_name,
			a.attname AS column_name,
			ix.indisunique,
			ix.indisprimary,
			am.amname
		FROM pg_index ix
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ak(attnum, ord) ON TRUE
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ak.attnum
		WHERE n.nspname = $1 AND t.relname = $2
		ORDER BY i.relname, ak.ord
	`
	rows, err := c.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*Index)
	for rows.Next() {
		var idxName, colName string
		var isUnique, isPrimary bool
		var amName string
		if err := rows.Scan(&idxName, &colName, &isUnique, &isPrimary, &amName); err != nil {
			return nil, err
		}
		if _, ok := indexMap[idxName]; !ok {
			indexMap[idxName] = &Index{
				TableName: table,
				IndexName: idxName,
				IsUnique:  isUnique,
				IsPrimary: isPrimary,
				IndexType: amName,
			}
		}
		indexMap[idxName].Columns = append(indexMap[idxName].Columns, colName)
	}

	var result []Index
	for _, idx := range indexMap {
		result = append(result, *idx)
	}
	return result, nil
}

func (c *Collector) collectForeignKeys(ctx context.Context, schema, table string) ([]ForeignKey, error) {
	query := `
		SELECT
			tc.constraint_name,
			kcu.column_name,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column,
			COALESCE(rc.delete_rule, 'NO ACTION'),
			COALESCE(rc.update_rule, 'NO ACTION')
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
		LEFT JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1 AND tc.table_name = $2
		ORDER BY tc.constraint_name, kcu.ordinal_position
	`
	rows, err := c.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKey)
	for rows.Next() {
		var constraintName, colName, refTable, refCol, onDelete, onUpdate string
		if err := rows.Scan(&constraintName, &colName, &refTable, &refCol, &onDelete, &onUpdate); err != nil {
			return nil, err
		}
		if _, ok := fkMap[constraintName]; !ok {
			fkMap[constraintName] = &ForeignKey{
				ConstraintName: constraintName,
				TableName:      table,
				RefTable:       refTable,
				OnDelete:       onDelete,
				OnUpdate:       onUpdate,
			}
		}
		fkMap[constraintName].Columns = append(fkMap[constraintName].Columns, colName)
		fkMap[constraintName].RefColumns = append(fkMap[constraintName].RefColumns, refCol)
	}

	var result []ForeignKey
	for _, fk := range fkMap {
		result = append(result, *fk)
	}
	return result, nil
}

func (c *Collector) collectTableComment(ctx context.Context, schema, table string) (string, error) {
	var comment sql.NullString
	query := `
		SELECT obj_description(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`
	err := c.db.QueryRowContext(ctx, query, schema, table).Scan(&comment)
	if err != nil {
		return "", err
	}
	return comment.String, nil
}

func (c *Collector) CollectViews(ctx context.Context, schema string) ([]View, error) {
	query := `
		SELECT viewname, definition
		FROM pg_views
		WHERE schemaname = $1
		ORDER BY viewname
	`
	rows, err := c.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []View
	for rows.Next() {
		var v View
		if err := rows.Scan(&v.Name, &v.Definition); err != nil {
			return nil, err
		}
		v.Schema = schema
		views = append(views, v)
	}
	return views, nil
}

func (c *Collector) CollectEnums(ctx context.Context, schema string) ([]EnumType, error) {
	query := `
		SELECT t.typname, e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = $1
		ORDER BY t.typname, e.enumsortorder
	`
	rows, err := c.db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	enumMap := make(map[string][]string)
	for rows.Next() {
		var typeName, value string
		if err := rows.Scan(&typeName, &value); err != nil {
			return nil, err
		}
		enumMap[typeName] = append(enumMap[typeName], value)
	}

	var enums []EnumType
	for name, values := range enumMap {
		enums = append(enums, EnumType{Schema: schema, Name: name, Values: values})
	}
	return enums, nil
}

func (c *Collector) CollectUnsupported(ctx context.Context, schema string) ([]Object, error) {
	var unsupported []Object

	triggerQuery := `
		SELECT trigger_name, event_object_table
		FROM information_schema.triggers
		WHERE trigger_schema = $1
	`
	rows, err := c.db.QueryContext(ctx, triggerQuery, schema)
	if err == nil {
		for rows.Next() {
			var name, tableName string
			if rows.Scan(&name, &tableName) == nil {
				unsupported = append(unsupported, Object{
					Schema: schema, Name: name, Type: ObjTrigger,
					Unsupported: true, Note: fmt.Sprintf("trigger on %s", tableName),
				})
			}
		}
		rows.Close()
	}

	funcQuery := `
		SELECT routine_name, routine_type
		FROM information_schema.routines
		WHERE routine_schema = $1 AND routine_type = 'FUNCTION'
	`
	rows2, err := c.db.QueryContext(ctx, funcQuery, schema)
	if err == nil {
		for rows2.Next() {
			var name, rtype string
			if rows2.Scan(&name, &rtype) == nil {
				unsupported = append(unsupported, Object{
					Schema: schema, Name: name, Type: ObjFunction,
					Unsupported: true, Note: "stored functions not supported in TiDB",
				})
			}
		}
		rows2.Close()
	}

	return unsupported, nil
}

func resolvePGType(dataType, udtName string) PGType {
	switch dataType {
	case "smallint":
		return PGSmallint
	case "integer":
		return PGInteger
	case "bigint":
		return PGBigint
	case "real":
		return PGReal
	case "double precision":
		return PGDouble
	case "numeric":
		return PGNumeric
	case "decimal":
		return PGDecimal
	case "money":
		return PGMoney
	case "character varying":
		return PGVarchar
	case "character":
		return PGChar
	case "text":
		return PGText
	case "bytea":
		return PGBytea
	case "boolean":
		return PGBoolean
	case "date":
		return PGDate
	case "time without time zone":
		return PGTime
	case "time with time zone":
		return PGTimeTZ
	case "timestamp without time zone":
		return PGTimestamp
	case "timestamp with time zone":
		return PGTimestampTZ
	case "interval":
		return PGInterval
	case "json":
		return PGJSON
	case "jsonb":
		return PGJSONB
	case "uuid":
		return PGUUID
	case "macaddr":
		return PGMacaddr
	case "inet":
		return PGInet
	case "bit":
		return PGBit
	case "bit varying":
		return PGVarbit
	case "xml":
		return PGXML
	case "USER-DEFINED":
		if strings.HasPrefix(udtName, "_") {
			return PGArray
		}
		return PGUserDefined
	case "ARRAY":
		return PGArray
	}
	return PGType(dataType)
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
