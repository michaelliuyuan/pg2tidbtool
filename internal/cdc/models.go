// Package cdc implements PostgreSQL → TiDB change data capture (CDC)
// incremental sync using PG logical replication (pgoutput plugin).
package cdc

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// EventKind categorizes a CDC change event.
type EventKind string

const (
	EventInsert   EventKind = "insert"
	EventUpdate   EventKind = "update"
	EventDelete   EventKind = "delete"
	EventDDL      EventKind = "ddl"
	EventTruncate EventKind = "truncate"
)

// CDCEvent is a unified change event produced by the Transformer and consumed
// by downstream appliers.
type CDCEvent struct {
	LSN        pglogrepl.LSN `json:"lsn"`
	Timestamp  time.Time     `json:"timestamp"`
	Kind       EventKind     `json:"kind"`
	Schema     string        `json:"schema"`
	Table      string        `json:"table"`
	Columns    []ColumnValue `json:"columns,omitempty"`     // INSERT / UPDATE / DELETE
	OldColumns []ColumnValue `json:"old_columns,omitempty"` // UPDATE old values
	DDL        string        `json:"ddl,omitempty"`         // DDL statement text
	RawData    []byte        `json:"-"`                     // raw pgoutput message (for replay)
}

// ColumnValue is a single column name/value pair in a CDC event.
type ColumnValue struct {
	Name      string      `json:"name"`
	Value     interface{} `json:"value"`
	Type      string      `json:"type"`                // PG data type OID string
	IsKey     bool        `json:"is_key,omitempty"`    // part of the relation PK / replica identity (from RelationMessage KeyColumn flag)
	Unchanged bool        `json:"unchanged,omitempty"` // pgoutput 'u': unchanged TOASTed value, not sent — must never be rendered as a literal
}

// Checkpoint records the last successfully processed LSN.
type Checkpoint struct {
	LSN       pglogrepl.LSN `json:"lsn"`
	Timestamp time.Time     `json:"timestamp"`
	SlotName  string        `json:"slot_name"`
	// LastDDLID is the last applied pg2tidb_ddl_log.id (DDL replication resume,
	// #t59). At-least-once: on restart, DDL poll resumes from here so already-
	// applied DDL isn't replayed.
	LastDDLID int64 `json:"last_ddl_id,omitempty"`
}

// SourceConfig configures the PG logical replication source.
type SourceConfig struct {
	// Connection
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	SSLMode  string `json:"sslmode"`

	// Replication
	SlotName     string `json:"slot_name"`     // replication slot name
	Publication  string `json:"publication"`   // publication name
	OutputPlugin string `json:"output_plugin"` // default "pgoutput"

	// Tables to replicate (schema.table format)
	Tables        []string `json:"tables"`
	ExcludeTables []string `json:"exclude_tables"`

	// Checkpoint
	CheckpointFile string `json:"checkpoint_file"` // LSN checkpoint file path
}

// DefaultSourceConfig returns a SourceConfig with sensible defaults.
func DefaultSourceConfig() SourceConfig {
	return SourceConfig{
		SSLMode:        "disable",
		SlotName:       "pg2tidb_cdc",
		Publication:    "pg2tidb_pub",
		OutputPlugin:   "pgoutput",
		CheckpointFile: ".cdc_checkpoint.json",
	}
}

// TransformerConfig configures event transformation behavior.
type TransformerConfig struct {
	// IncludeOldValues controls whether UPDATE events carry old column values.
	IncludeOldValues bool `json:"include_old_values"`

	// MaxColumnValueLength truncates large column values (0 = no limit).
	MaxColumnValueLength int `json:"max_column_value_length"`
}

// DefaultTransformerConfig returns sensible defaults.
func DefaultTransformerConfig() TransformerConfig {
	return TransformerConfig{
		IncludeOldValues:     true,
		MaxColumnValueLength: 0,
	}
}
