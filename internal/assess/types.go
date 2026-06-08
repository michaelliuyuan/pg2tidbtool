package assess

import "fmt"

// Compatibility levels for findings.
const (
	LevelCompatible    = "compatible"     // ✅ Direct support
	LevelConvertible   = "convertible"    // ⚠️ Auto-mapped
	LevelManualNeeded  = "manual_needed"  // 🟡 Needs manual intervention
	LevelIncompatible  = "incompatible"   // ❌ Not supported
)

// Assessment dimensions with weights.
const (
	DimDataType  = "data_type"  // 25%
	DimStructure = "structure"  // 20%
	DimIndex     = "index"      // 15%
	DimView      = "view"       // 10%
	DimFunction  = "function"   // 10%
	DimTrigger   = "trigger"    // 5%
	DimCustomType = "custom_type" // 5%
	DimExtension = "extension"   // 5%
	DimSequence  = "sequence"    // 5%
)

// DimensionWeights maps each dimension to its weight in the overall score.
var DimensionWeights = map[string]float64{
	DimDataType:   0.25,
	DimStructure:  0.20,
	DimIndex:      0.15,
	DimView:       0.10,
	DimFunction:   0.10,
	DimTrigger:    0.05,
	DimCustomType: 0.05,
	DimExtension:  0.05,
	DimSequence:   0.05,
}

// LevelScore maps compatibility levels to numeric scores.
var LevelScore = map[string]float64{
	LevelCompatible:   100,
	LevelConvertible:  75,
	LevelManualNeeded: 40,
	LevelIncompatible: 0,
}

// LevelEmoji maps compatibility levels to display emoji.
var LevelEmoji = map[string]string{
	LevelCompatible:   "✅",
	LevelConvertible:  "⚠️",
	LevelManualNeeded: "🟡",
	LevelIncompatible: "❌",
}

// ScanResult holds all scanned schema objects from PostgreSQL.
type ScanResult struct {
	Tables    []TableInfo
	Columns   []ColumnInfo
	Indexes   []IndexInfo
	Views     []ViewInfo
	Functions []FunctionInfo
	Triggers  []TriggerInfo
	Enums     []EnumInfo
	Extensions []ExtensionInfo
	Sequences []SequenceInfo
}

// TableInfo represents a PG table.
type TableInfo struct {
	Schema string
	Name   string
}

// ColumnInfo represents a PG column with its type info.
type ColumnInfo struct {
	TableSchema    string
	TableName      string
	ColumnName     string
	DataType       string // PG data type name
	MaxLength      int    // character_maximum_length
	NumericPrec    int    // numeric_precision
	NumericScale   int    // numeric_scale
	IsNullable     bool
	ColumnDefault  string
	IsPrimary      bool
	OrdinalPosition int
}

// IndexInfo represents a PG index.
type IndexInfo struct {
	TableName  string
	Name       string
	IndexType  string // btree, hash, gin, gist, brin, spgist
	IsUnique   bool
	IsPrimary  bool
	Definition string
	IsPartial  bool // has WHERE clause
	IsExpression bool // uses expression
	DDL        string // Full CREATE INDEX DDL
}

// ViewInfo represents a PG view.
type ViewInfo struct {
	Schema     string
	Name       string
	Definition string
	DDL        string // Full CREATE VIEW DDL
}

// FunctionInfo represents a PG function/procedure.
type FunctionInfo struct {
	Schema      string
	Name        string
	ReturnType  string
	Language    string
	Source      string
	IsProcedure bool
	DDL         string // Full CREATE FUNCTION/PROCEDURE DDL
}

// TriggerInfo represents a PG trigger.
type TriggerInfo struct {
	TableName     string
	Name          string
	EventType     string // INSERT, UPDATE, DELETE, TRUNCATE
	Timing        string // BEFORE, AFTER, INSTEAD OF
	Statement     string
	DDL           string // Full CREATE TRIGGER DDL
}

// EnumInfo represents a PG enum type.
type EnumInfo struct {
	Schema  string
	Name    string
	Values  []string
	DDL     string // Full CREATE TYPE AS ENUM DDL
}

// ExtensionInfo represents a PG extension.
type ExtensionInfo struct {
	Name    string
	Version string
	Installed bool
	DDL      string // Full CREATE EXTENSION DDL
}

// SequenceInfo represents a PG sequence.
type SequenceInfo struct {
	Schema    string
	Name      string
	DataType  string
	StartValue int64
	Increment  int64
	MaxValue   int64
	MinValue   int64
	DDL        string // Full CREATE SEQUENCE DDL
}

// Finding represents a single compatibility assessment result.
type Finding struct {
	Dimension   string `json:"dimension"`
	ObjectType  string `json:"object_type"`
	ObjectName  string `json:"object_name"`
	Level       string `json:"level"`
	PGDetail    string `json:"pg_detail"`
	TiDBDetail  string `json:"tidb_detail"`
	Suggestion  string `json:"suggestion"`
	AutoFix     bool   `json:"auto_fix"`
	DDL         string `json:"ddl,omitempty"`         // Original DDL from PG
	TiDBDDL     string `json:"tidb_ddl,omitempty"`    // Suggested TiDB-compatible DDL
}

// DimensionResult holds the assessment results for one dimension.
type DimensionResult struct {
	Dimension string   `json:"dimension"`
	Total     int      `json:"total"`
	Score     float64  `json:"score"`
	Findings  []Finding `json:"findings"`
}

// AssessmentReport is the top-level report.
type AssessmentReport struct {
	Score            float64            `json:"score"`
	Level            string             `json:"level"`
	DimensionResults []DimensionResult  `json:"dimension_results"`
	AllFindings      []Finding          `json:"all_findings"`
	Summary          map[string]int     `json:"summary"`
}

// Score calculates the overall score from dimension results.
func (r *AssessmentReport) Score_() float64 {
	if len(r.DimensionResults) == 0 {
		return 0
	}
	var total float64
	for _, dr := range r.DimensionResults {
		weight := DimensionWeights[dr.Dimension]
		total += weight * dr.Score
	}
	return total
}

// OverallLevel returns the compatibility level based on score.
func OverallLevel(score float64) string {
	switch {
	case score >= 90:
		return LevelCompatible
	case score >= 70:
		return LevelConvertible
	case score >= 40:
		return LevelManualNeeded
	default:
		return LevelIncompatible
	}
}

// FormatScore returns a human-readable score string.
func FormatScore(score float64) string {
	return fmt.Sprintf("%.1f", score)
}
