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
}

// ViewInfo represents a PG view.
type ViewInfo struct {
	Schema     string
	Name       string
	Definition string
}

// FunctionInfo represents a PG function/procedure.
type FunctionInfo struct {
	Schema      string
	Name        string
	ReturnType  string
	Language    string
	Source      string
	IsProcedure bool
}

// TriggerInfo represents a PG trigger.
type TriggerInfo struct {
	TableName     string
	Name          string
	EventType     string // INSERT, UPDATE, DELETE, TRUNCATE
	Timing        string // BEFORE, AFTER, INSTEAD OF
	Statement     string
}

// EnumInfo represents a PG enum type.
type EnumInfo struct {
	Schema  string
	Name    string
	Values  []string
}

// ExtensionInfo represents a PG extension.
type ExtensionInfo struct {
	Name    string
	Version string
	Installed bool
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
}

// Finding represents a single compatibility assessment result.
type Finding struct {
	Dimension   string // assessment dimension
	ObjectType  string // "column", "index", "view", "function", etc.
	ObjectName  string // fully qualified name
	Level       string // compatibility level
	PGDetail    string // PG-side detail
	TiDBDetail  string // TiDB-side detail
	Suggestion  string // migration suggestion
	AutoFix     bool   // whether this can be auto-fixed
}

// DimensionResult holds the assessment results for one dimension.
type DimensionResult struct {
	Dimension string
	Total     int
	Score     float64 // 0-100
	Findings  []Finding
}

// AssessmentReport is the top-level report.
type AssessmentReport struct {
	Score          float64            // overall weighted score 0-100
	Level          string             // overall compatibility level
	DimensionResults []DimensionResult
	AllFindings    []Finding
	Summary        map[string]int     // level -> count
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
