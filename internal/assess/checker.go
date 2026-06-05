package assess

import (
	"fmt"
	"strings"
)

// Assessor runs all compatibility checks on a ScanResult.
type Assessor struct{}

// NewAssessor creates a new Assessor.
func NewAssessor() *Assessor {
	return &Assessor{}
}

// Assess runs all checkers and returns dimension results.
func (a *Assessor) Assess(result *ScanResult) []DimensionResult {
	var dims []DimensionResult

	dims = append(dims, a.checkDataTypes(result))
	dims = append(dims, a.checkStructure(result))
	dims = append(dims, a.checkIndexes(result))
	dims = append(dims, a.checkViews(result))
	dims = append(dims, a.checkFunctions(result))
	dims = append(dims, a.checkTriggers(result))
	dims = append(dims, a.checkCustomTypes(result))
	dims = append(dims, a.checkExtensions(result))
	dims = append(dims, a.checkSequences(result))

	return dims
}

// scorer helps build dimension results.
type scorer struct {
	findings []Finding
}

func (s *scorer) add(f Finding) {
	s.findings = append(s.findings, f)
}

func (s *scorer) result(dimension string) DimensionResult {
	if len(s.findings) == 0 {
		return DimensionResult{
			Dimension: dimension,
			Total:     0,
			Score:     100,
			Findings:  nil,
		}
	}
	var totalScore float64
	for _, f := range s.findings {
		totalScore += LevelScore[f.Level]
	}
	avg := totalScore / float64(len(s.findings))

	return DimensionResult{
		Dimension: dimension,
		Total:     len(s.findings),
		Score:     avg,
		Findings:  s.findings,
	}
}

// --- Data Types Checker ---

func (a *Assessor) checkDataTypes(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, col := range result.Columns {
		dt := strings.ToLower(col.DataType)
		objName := fmt.Sprintf("%s.%s.%s", col.TableSchema, col.TableName, col.ColumnName)

		switch {
		// Fully compatible types
		case dt == "integer" || dt == "int" || dt == "int4":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "integer", TiDBDetail: "INT",
				Suggestion: "", AutoFix: true,
			})
		case dt == "bigint" || dt == "int8":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "bigint", TiDBDetail: "BIGINT",
				Suggestion: "", AutoFix: true,
			})
		case dt == "smallint" || dt == "int2":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "smallint", TiDBDetail: "SMALLINT",
				Suggestion: "", AutoFix: true,
			})
		case dt == "text" || dt == "character varying" || dt == "varchar":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: dt, TiDBDetail: "TEXT/VARCHAR",
				Suggestion: "", AutoFix: true,
			})
		case dt == "character" || dt == "bpchar":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: fmt.Sprintf("CHAR(%d)", col.MaxLength), TiDBDetail: "CHAR",
				Suggestion: "CHAR 类型尾部空格处理可能不同", AutoFix: true,
			})
		case dt == "numeric" || dt == "decimal":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: fmt.Sprintf("NUMERIC(%d,%d)", col.NumericPrec, col.NumericScale),
				TiDBDetail: "DECIMAL",
				Suggestion: "", AutoFix: true,
			})
		case dt == "real" || dt == "float4":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "real", TiDBDetail: "FLOAT",
				Suggestion: "", AutoFix: true,
			})
		case dt == "double precision" || dt == "float8":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "double precision", TiDBDetail: "DOUBLE",
				Suggestion: "", AutoFix: true,
			})
		case dt == "boolean" || dt == "bool":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: "boolean", TiDBDetail: "TINYINT(1)",
				Suggestion: "布尔值 true/false → 1/0", AutoFix: true,
			})
		case dt == "date":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "date", TiDBDetail: "DATE",
				Suggestion: "", AutoFix: true,
			})
		case dt == "timestamp without time zone" || dt == "timestamp":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "timestamp", TiDBDetail: "DATETIME",
				Suggestion: "", AutoFix: true,
			})
		case dt == "timestamp with time zone" || dt == "timestamptz":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: "timestamptz", TiDBDetail: "TIMESTAMP",
				Suggestion: "时区信息会丢失，建议应用层处理时区转换", AutoFix: true,
			})
		case dt == "bytea":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "bytea", TiDBDetail: "BLOB",
				Suggestion: "", AutoFix: true,
			})
		case dt == "jsonb":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: "jsonb", TiDBDetail: "JSON",
				Suggestion: "TiDB JSON 为文本存储，无二进制优化", AutoFix: true,
			})
		case dt == "json":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "json", TiDBDetail: "JSON",
				Suggestion: "", AutoFix: true,
			})
		case dt == "uuid":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: "uuid", TiDBDetail: "CHAR(36) 或 BINARY(16)",
				Suggestion: "建议用 CHAR(36) 存储 UUID 字符串格式", AutoFix: true,
			})
		case dt == "bit" || dt == "bit varying" || dt == "varbit":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: dt, TiDBDetail: "无直接对应",
				Suggestion: "建议用 BINARY/VARBINARY 或 BIGINT 替代", AutoFix: false,
			})
		case dt == "money":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: "money", TiDBDetail: "DECIMAL(19,2)",
				Suggestion: "", AutoFix: true,
			})
		case dt == "xml":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: "xml", TiDBDetail: "TEXT 或 JSON",
				Suggestion: "TiDB 无原生 XML 支持，建议用 TEXT 存储", AutoFix: false,
			})
		case dt == "cidr" || dt == "inet":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: dt, TiDBDetail: "VARCHAR(45)",
				Suggestion: "建议用 VARCHAR 存储 IP 地址", AutoFix: false,
			})
		case dt == "macaddr":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: "macaddr", TiDBDetail: "VARCHAR(17)",
				Suggestion: "建议用 VARCHAR 存储 MAC 地址", AutoFix: false,
			})
		case dt == "interval":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: "interval", TiDBDetail: "不支持",
				Suggestion: "TiDB 不支持 INTERVAL 类型，需改为 VARCHAR 或 INT（秒数）", AutoFix: false,
			})
		case dt == "point" || dt == "line" || dt == "lseg" || dt == "box" ||
			dt == "path" || dt == "polygon" || dt == "circle":
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: dt, TiDBDetail: "不支持原生几何类型",
				Suggestion: "可考虑用 TiDB 空间函数 + GEOMETRY 替代", AutoFix: false,
			})
		case strings.HasSuffix(dt, "[]"):
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: fmt.Sprintf("ARRAY(%s)", strings.TrimSuffix(dt, "[]")),
				TiDBDetail: "JSON",
				Suggestion: "PG 数组需转为 JSON 格式存储", AutoFix: false,
			})
		case strings.HasPrefix(dt, "user-defined"):
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: dt, TiDBDetail: "需评估",
				Suggestion: "自定义类型需手动映射", AutoFix: false,
			})
		default:
			// Types we don't explicitly handle
			s.add(Finding{
				Dimension: DimDataType, ObjectType: "column",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: dt, TiDBDetail: "需评估",
				Suggestion: fmt.Sprintf("数据类型 %s 需手动评估兼容性", dt), AutoFix: false,
			})
		}
	}

	return s.result(DimDataType)
}

// --- Structure Checker ---

func (a *Assessor) checkStructure(result *ScanResult) DimensionResult {
	s := &scorer{}

	// Check for tables without PK
	for _, t := range result.Tables {
		hasPK := false
		for _, c := range result.Columns {
			if c.TableName == t.Name && c.IsPrimary {
				hasPK = true
				break
			}
		}
		if !hasPK {
			s.add(Finding{
				Dimension: DimStructure, ObjectType: "table",
				ObjectName: fmt.Sprintf("%s.%s", t.Schema, t.Name),
				Level: LevelManualNeeded,
				PGDetail: "无主键", TiDBDetail: "建议添加主键",
				Suggestion: "TiDB 强烈建议每张表都有主键，影响性能和数据校验", AutoFix: false,
			})
		}
	}

	// Check column defaults with PG-specific functions
	for _, col := range result.Columns {
		if col.ColumnDefault == "" {
			continue
		}
		def := strings.ToLower(col.ColumnDefault)
		objName := fmt.Sprintf("%s.%s.%s", col.TableSchema, col.TableName, col.ColumnName)

		switch {
		case strings.Contains(def, "nextval(") || strings.Contains(def, "nextval("):
			// Sequence default — handled by sequence checker
		case strings.Contains(def, "now()") || strings.Contains(def, "current_timestamp"):
			s.add(Finding{
				Dimension: DimStructure, ObjectType: "column_default",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: def, TiDBDetail: "CURRENT_TIMESTAMP / NOW()",
				Suggestion: "TiDB 支持 NOW() 和 CURRENT_TIMESTAMP", AutoFix: true,
			})
		case strings.Contains(def, "uuid_generate_v4()") || strings.Contains(def, "gen_random_uuid()"):
			s.add(Finding{
				Dimension: DimStructure, ObjectType: "column_default",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: def, TiDBDetail: "无内置 UUID 生成",
				Suggestion: "TiDB 无内置 UUID 生成函数，建议应用层生成或用 REPLACE(UUID(),'-','')", AutoFix: false,
			})
		}
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimStructure, Score: 100}
	}
	return s.result(DimStructure)
}

// --- Index Checker ---

func (a *Assessor) checkIndexes(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, idx := range result.Indexes {
		idxType := strings.ToLower(idx.IndexType)
		objName := fmt.Sprintf("%s.%s", idx.TableName, idx.Name)

		switch {
		case idxType == "btree":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "B-tree", TiDBDetail: "B-tree",
				Suggestion: "", AutoFix: true,
			})
		case idxType == "hash":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: "HASH", TiDBDetail: "不支持",
				Suggestion: "HASH 索引 TiDB 不支持，建议改用 B-tree", AutoFix: false,
			})
		case idxType == "gin":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: "GIN", TiDBDetail: "不支持",
				Suggestion: "GIN 索引不支持，JSON 查询需改用 TiDB JSON 函数", AutoFix: false,
			})
		case idxType == "gist":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: "GiST", TiDBDetail: "不支持",
				Suggestion: "GiST 索引不支持，几何/全文索引需替代方案", AutoFix: false,
			})
		case idxType == "brin":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: "BRIN", TiDBDetail: "不支持",
				Suggestion: "BRIN 索引不支持，建议使用分区表替代", AutoFix: false,
			})
		case idxType == "spgist":
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelIncompatible,
				PGDetail: "SP-GiST", TiDBDetail: "不支持",
				Suggestion: "SP-GiST 索引不支持", AutoFix: false,
			})
		default:
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: idxType, TiDBDetail: "需评估",
				Suggestion: fmt.Sprintf("索引类型 %s 需手动评估", idxType), AutoFix: false,
			})
		}

		if idx.IsPartial {
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: fmt.Sprintf("%s (部分索引)", objName),
				Level: LevelIncompatible,
				PGDetail: "部分索引 (WHERE 子句)", TiDBDetail: "不支持",
				Suggestion: "部分索引不支持，考虑使用分区表或覆盖索引替代", AutoFix: false,
			})
		}

		if idx.IsExpression {
			s.add(Finding{
				Dimension: DimIndex, ObjectType: "index",
				ObjectName: fmt.Sprintf("%s (表达式索引)", objName),
				Level: LevelManualNeeded,
				PGDetail: "表达式索引", TiDBDetail: "不支持",
				Suggestion: "表达式索引不支持，需用生成列+索引替代", AutoFix: false,
			})
		}
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimIndex, Score: 100}
	}
	return s.result(DimIndex)
}

// --- View Checker ---

func (a *Assessor) checkViews(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, view := range result.Views {
		objName := fmt.Sprintf("%s.%s", view.Schema, view.Name)
		def := strings.ToUpper(view.Definition)

		issues := 0
		// Check for PG-specific syntax
		pgSyntax := []string{
			"ARRAY_AGG", "STRING_AGG", "BOOL_AND", "BOOL_OR",
			"EXTRACT(EPOCH", "TO_CHAR", "TO_NUMBER", "TO_DATE",
			"REGEXP_REPLACE", "REGEXP_MATCHES",
			"GENERATE_SERIES", "ROW_NUMBER()",
			"LATERAL", "WITH RECURSIVE",
			"ILIKE", "SIMILAR TO",
			"::", "CAST(", "NOW()", "CURRENT_DATE",
		}

		for _, syntax := range pgSyntax {
			if strings.Contains(def, syntax) {
				issues++
			}
		}

		switch {
		case issues == 0:
			s.add(Finding{
				Dimension: DimView, ObjectType: "view",
				ObjectName: objName,
				Level: LevelCompatible,
				PGDetail: "简单视图", TiDBDetail: "兼容",
				Suggestion: "", AutoFix: true,
			})
		case issues <= 2:
			s.add(Finding{
				Dimension: DimView, ObjectType: "view",
				ObjectName: objName,
				Level: LevelConvertible,
				PGDetail: fmt.Sprintf("含 %d 个 PG 特有语法", issues), TiDBDetail: "需改写",
				Suggestion: "部分函数需要手动改写", AutoFix: false,
			})
		default:
			s.add(Finding{
				Dimension: DimView, ObjectType: "view",
				ObjectName: objName,
				Level: LevelManualNeeded,
				PGDetail: fmt.Sprintf("含 %d 个 PG 特有语法", issues), TiDBDetail: "需大量改写",
				Suggestion: "视图需要大量改写，建议逐步迁移", AutoFix: false,
			})
		}
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimView, Score: 100}
	}
	return s.result(DimView)
}

// --- Function Checker ---

func (a *Assessor) checkFunctions(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, fn := range result.Functions {
		objName := fmt.Sprintf("%s.%s", fn.Schema, fn.Name)
		s.add(Finding{
			Dimension: DimFunction, ObjectType: "function",
			ObjectName: objName,
			Level: LevelIncompatible,
			PGDetail: fmt.Sprintf("%s (%s)", fn.Language, fn.ReturnType),
			TiDBDetail: "不支持 PL/pgSQL",
			Suggestion: fmt.Sprintf("函数 %s 需要在应用层重新实现", fn.Name),
			AutoFix: false,
		})
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimFunction, Score: 100}
	}
	return s.result(DimFunction)
}

// --- Trigger Checker ---

func (a *Assessor) checkTriggers(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, trig := range result.Triggers {
		objName := fmt.Sprintf("%s.%s", trig.TableName, trig.Name)
		s.add(Finding{
			Dimension: DimTrigger, ObjectType: "trigger",
			ObjectName: objName,
			Level: LevelIncompatible,
			PGDetail: fmt.Sprintf("%s %s", trig.Timing, trig.EventType),
			TiDBDetail: "不支持",
			Suggestion: "TiDB 不支持触发器，需在应用层实现业务逻辑", AutoFix: false,
		})
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimTrigger, Score: 100}
	}
	return s.result(DimTrigger)
}

// --- Custom Type Checker ---

func (a *Assessor) checkCustomTypes(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, enum := range result.Enums {
		objName := fmt.Sprintf("%s.%s", enum.Schema, enum.Name)
		s.add(Finding{
			Dimension: DimCustomType, ObjectType: "enum",
			ObjectName: objName,
			Level: LevelConvertible,
			PGDetail: fmt.Sprintf("ENUM(%s)", strings.Join(enum.Values, ",")),
			TiDBDetail: "ENUM",
			Suggestion: "TiDB 支持 ENUM 类型，可直接映射", AutoFix: true,
		})
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimCustomType, Score: 100}
	}
	return s.result(DimCustomType)
}

// --- Extension Checker ---

func (a *Assessor) checkExtensions(result *ScanResult) DimensionResult {
	s := &scorer{}

	knownExtensions := map[string]struct {
		level      string
		tidbDetail string
		suggestion string
	}{
		"plpgsql":    {LevelIncompatible, "不支持", "存储过程需应用层实现"},
		"uuid-ossp":  {LevelManualNeeded, "无内置", "用 REPLACE(UUID(),'-','') 替代"},
		"pgcrypto":   {LevelManualNeeded, "无内置", "需应用层加密"},
		"pg_stat_statements": {LevelIncompatible, "无直接对应", "TiDB 有内置监控"},
		"postgis":    {LevelIncompatible, "有限支持", "TiDB 有基本空间函数"},
		"hstore":     {LevelManualNeeded, "无内置", "建议用 JSON 替代"},
		"ltree":      {LevelIncompatible, "不支持", "需应用层实现"},
		"pg_trgm":    {LevelIncompatible, "不支持", "用 TiDB 全文索引替代"},
		"btree_gin":  {LevelIncompatible, "不支持", ""},
		"btree_gist": {LevelIncompatible, "不支持", ""},
	}

	for _, ext := range result.Extensions {
		if info, ok := knownExtensions[ext.Name]; ok {
			s.add(Finding{
				Dimension: DimExtension, ObjectType: "extension",
				ObjectName: ext.Name,
				Level: info.level,
				PGDetail: ext.Name, TiDBDetail: info.tidbDetail,
				Suggestion: info.suggestion, AutoFix: false,
			})
		} else {
			s.add(Finding{
				Dimension: DimExtension, ObjectType: "extension",
				ObjectName: ext.Name,
				Level: LevelManualNeeded,
				PGDetail: ext.Name, TiDBDetail: "需评估",
				Suggestion: fmt.Sprintf("扩展 %s 需手动评估替代方案", ext.Name),
				AutoFix: false,
			})
		}
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimExtension, Score: 100}
	}
	return s.result(DimExtension)
}

// --- Sequence Checker ---

func (a *Assessor) checkSequences(result *ScanResult) DimensionResult {
	s := &scorer{}

	for _, seq := range result.Sequences {
		objName := fmt.Sprintf("%s.%s", seq.Schema, seq.Name)
		s.add(Finding{
			Dimension: DimSequence, ObjectType: "sequence",
			ObjectName: objName,
			Level: LevelConvertible,
			PGDetail: "SEQUENCE", TiDBDetail: "AUTO_INCREMENT",
			Suggestion: "序列需改为 AUTO_INCREMENT 列", AutoFix: true,
		})
	}

	if len(s.findings) == 0 {
		return DimensionResult{Dimension: DimSequence, Score: 100}
	}
	return s.result(DimSequence)
}
