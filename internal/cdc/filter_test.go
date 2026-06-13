package cdc

import (
	"testing"
)

func TestTableFilter_AllowAll(t *testing.T) {
	f := NewTableFilter()

	tests := []struct{ schema, table string }{
		{"public", "users"},
		{"public", "orders"},
		{"myschema", "products"},
	}

	for _, tt := range tests {
		if !f.Allow(tt.schema, tt.table) {
			t.Errorf("Allow(%q, %q) = false, want true", tt.schema, tt.table)
		}
	}
}

func TestTableFilter_Blacklist(t *testing.T) {
	f := NewTableFilter().WithBlacklist([]string{"public.temp_table", "public.*_log"})

	if !f.Allow("public", "users") {
		t.Error("users should be allowed")
	}
	if f.Allow("public", "temp_table") {
		t.Error("temp_table should be excluded")
	}
	if f.Allow("public", "audit_log") {
		t.Error("audit_log should be excluded by *_log pattern")
	}
}

func TestTableFilter_Whitelist(t *testing.T) {
	f := NewTableFilter().WithWhitelist([]string{"public.users", "public.orders"})

	if !f.Allow("public", "users") {
		t.Error("users should be allowed (in whitelist)")
	}
	if f.Allow("public", "products") {
		t.Error("products should be excluded (not in whitelist)")
	}
}

func TestTableFilter_SchemaExclude(t *testing.T) {
	f := NewTableFilter().WithSchemas(nil, []string{"pg_catalog", "information_schema"})

	if !f.Allow("public", "users") {
		t.Error("public.users should be allowed")
	}
	if f.Allow("pg_catalog", "pg_class") {
		t.Error("pg_catalog should be excluded")
	}
	if f.Allow("information_schema", "tables") {
		t.Error("information_schema should be excluded")
	}
}

func TestTableFilter_SchemaInclude(t *testing.T) {
	f := NewTableFilter().WithSchemas([]string{"public", "app"}, nil)

	if !f.Allow("public", "users") {
		t.Error("public.users should be allowed")
	}
	if !f.Allow("app", "settings") {
		t.Error("app.settings should be allowed")
	}
	if f.Allow("other", "data") {
		t.Error("other.data should be excluded (not in schema whitelist)")
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		s, pattern string
		want       bool
	}{
		{"users", "users", true},
		{"users", "orders", false},
		{"users_2024", "users_*", true},
		{"orders_archive_2024", "orders_*", true},
		{"public.users", "public.*", true},
		{"public.users", "*.users", true},
		{"public.users", "private.*", false},
	}

	for _, tt := range tests {
		got := matchPattern(tt.s, tt.pattern)
		if got != tt.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
		}
	}
}

func TestDDLTransformer_Table(t *testing.T) {
	dt := NewDDLTransformer()

	ddl := "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), data JSONB)"
	result := dt.Transform(ddl, "TABLE")

	// Should replace SERIAL with BIGINT AUTO_INCREMENT
	if !contains(result, "BIGINT AUTO_INCREMENT") {
		t.Errorf("expected BIGINT AUTO_INCREMENT, got: %s", result)
	}
	// Should replace JSONB with JSON
	if !contains(result, "JSON") {
		t.Errorf("expected JSON, got: %s", result)
	}
}

func TestDDLTransformer_Index(t *testing.T) {
	dt := NewDDLTransformer()

	// GIN should be commented out
	ddl := "CREATE INDEX idx_gin ON users USING GIN (data)"
	result := dt.Transform(ddl, "INDEX")
	if !contains(result, "not supported") {
		t.Errorf("expected 'not supported' for GIN index, got: %s", result)
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
