package cdc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestTransformer_Insert(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "name", Value: "Alice", Type: "oid_25"},
			{Name: "email", Value: "alice@example.com", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(insert): %v", err)
	}

	expected := "REPLACE INTO `users` (`id`, `name`, `email`) VALUES ('1', 'Alice', 'alice@example.com')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_Update(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventUpdate,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "name", Value: "Bob", Type: "oid_25"},
		},
		OldColumns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "name", Value: "Alice", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(update): %v", err)
	}

	expected := "UPDATE `users` SET `id` = '1', `name` = 'Bob' WHERE `id` = '1' AND `name` = 'Alice'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_Delete(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventDelete,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "42", Type: "oid_23"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(delete): %v", err)
	}

	expected := "DELETE FROM `users` WHERE `id` = '42'"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_Truncate(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventTruncate,
		Schema: "public",
		Table:  "users",
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(truncate): %v", err)
	}

	expected := "TRUNCATE TABLE `users`"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_NullValue(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "t",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "description", Value: nil, Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(null): %v", err)
	}

	expected := "REPLACE INTO `t` (`id`, `description`) VALUES ('1', NULL)"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_SpecialChars(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "public",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
			{Name: "bio", Value: "It's a \"test\"", Type: "oid_25"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(special): %v", err)
	}

	// Single quotes should be escaped
	expected := "REPLACE INTO `users` (`id`, `bio`) VALUES ('1', 'It''s a \"test\"')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestTransformer_SchemaQuoting(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())

	event := &CDCEvent{
		Kind:   EventInsert,
		Schema: "myschema",
		Table:  "users",
		Columns: []ColumnValue{
			{Name: "id", Value: "1", Type: "oid_23"},
		},
	}

	sql, err := tr.TransformEvent(event)
	if err != nil {
		t.Fatalf("TransformEvent(schema): %v", err)
	}

	expected := "REPLACE INTO `myschema`.`users` (`id`) VALUES ('1')"
	if sql != expected {
		t.Errorf("got:\n  %s\nwant:\n  %s", sql, expected)
	}
}

func TestCheckpointManager_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_checkpoint.json")

	cm := NewCheckpointManager(path)
	cm.SetSlotName("test_slot")
	cm.Update(pglogrepl.LSN(12345))

	if err := cm.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify file was written
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if cp.LSN != pglogrepl.LSN(12345) {
		t.Errorf("LSN = %d, want 12345", cp.LSN)
	}
	if cp.SlotName != "test_slot" {
		t.Errorf("SlotName = %q, want test_slot", cp.SlotName)
	}

	// Load into a new manager
	cm2 := NewCheckpointManager(path)
	loaded, err := cm2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint, got nil")
	}
	if loaded.LSN != pglogrepl.LSN(12345) {
		t.Errorf("loaded LSN = %d, want 12345", loaded.LSN)
	}
}

func TestCheckpointManager_LoadNonExistent(t *testing.T) {
	cm := NewCheckpointManager("/nonexistent/path/checkpoint.json")
	cp, err := cm.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp != nil {
		t.Errorf("expected nil checkpoint for non-existent file, got %v", cp)
	}
}

func TestCheckpointManager_DirtyFlag(t *testing.T) {
	cm := NewCheckpointManager(filepath.Join(t.TempDir(), "cp.json"))

	if cm.IsDirty() {
		t.Error("expected clean after creation")
	}

	cm.Update(pglogrepl.LSN(100))
	if !cm.IsDirty() {
		t.Error("expected dirty after Update")
	}

	cm.Save()
	if cm.IsDirty() {
		t.Error("expected clean after Save")
	}
}

func TestTransformer_UnknownEventKind(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{Kind: EventKind("unknown")}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for unknown event kind")
	}
}

func TestTransformer_UpdateWithoutColumns(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{
		Kind:  EventUpdate,
		Table: "users",
	}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for UPDATE without WHERE columns")
	}
}

func TestTransformer_DeleteWithoutColumns(t *testing.T) {
	tr := NewTransformer(DefaultTransformerConfig())
	event := &CDCEvent{
		Kind:  EventDelete,
		Table: "users",
	}
	_, err := tr.TransformEvent(event)
	if err == nil {
		t.Error("expected error for DELETE without WHERE columns")
	}
}

func TestSourceConfigDefaults(t *testing.T) {
	cfg := DefaultSourceConfig()
	if cfg.SlotName != "pg2tidb_cdc" {
		t.Errorf("SlotName = %q, want pg2tidb_cdc", cfg.SlotName)
	}
	if cfg.Publication != "pg2tidb_pub" {
		t.Errorf("Publication = %q, want pg2tidb_pub", cfg.Publication)
	}
	if cfg.OutputPlugin != "pgoutput" {
		t.Errorf("OutputPlugin = %q, want pgoutput", cfg.OutputPlugin)
	}
}
