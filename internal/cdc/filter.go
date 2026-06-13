package cdc

import (
	"strings"
	"sync"
)

// TableFilter controls which tables are included/excluded from replication.
type TableFilter struct {
	mu sync.RWMutex

	// IncludeTables is a whitelist of "schema.table" patterns. If non-empty,
	// only matching tables are replicated.
	IncludeTables []string

	// ExcludeTables is a blacklist of "schema.table" patterns. Tables matching
	// any exclude pattern are skipped.
	ExcludeTables []string

	// IncludeSchemas limits replication to specific schemas.
	IncludeSchemas []string

	// ExcludeSchemas excludes entire schemas from replication.
	ExcludeSchemas []string
}

// NewTableFilter creates a new table filter.
func NewTableFilter() *TableFilter {
	return &TableFilter{}
}

// WithWhitelist sets the whitelist (include) tables.
func (f *TableFilter) WithWhitelist(tables []string) *TableFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.IncludeTables = tables
	return f
}

// WithBlacklist sets the blacklist (exclude) tables.
func (f *TableFilter) WithBlacklist(tables []string) *TableFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ExcludeTables = tables
	return f
}

// WithSchemas sets schema-level filters.
func (f *TableFilter) WithSchemas(include, exclude []string) *TableFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.IncludeSchemas = include
	f.ExcludeSchemas = exclude
	return f
}

// Allow returns true if the given schema.table should be replicated.
func (f *TableFilter) Allow(schema, table string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := tableKey(schema, table)

	// Schema-level exclusion first
	for _, s := range f.ExcludeSchemas {
		if matchPattern(schema, s) {
			return false
		}
	}

	// Table-level exclusion
	for _, pattern := range f.ExcludeTables {
		if matchPattern(key, pattern) {
			return false
		}
	}

	// If whitelist is set, table must match
	if len(f.IncludeTables) > 0 {
		for _, pattern := range f.IncludeTables {
			if matchPattern(key, pattern) {
				return true
			}
		}
		return false
	}

	// If schema whitelist is set, schema must match
	if len(f.IncludeSchemas) > 0 {
		for _, s := range f.IncludeSchemas {
			if matchPattern(schema, s) {
				return true
			}
		}
		return false
	}

	return true
}

// AllowEvent is a convenience wrapper for Allow(event.Schema, event.Table).
func (f *TableFilter) AllowEvent(event *CDCEvent) bool {
	return f.Allow(event.Schema, event.Table)
}

// matchPattern does simple glob matching with * wildcard.
func matchPattern(s, pattern string) bool {
	// Exact match
	if pattern == s {
		return true
	}

	// Wildcard match: "users_*" matches "users_2024", "*" matches everything
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		pos := 0
		for i, part := range parts {
			if part == "" {
				if i == len(parts)-1 {
					// Trailing "*": match everything after
					return true
				}
				continue
			}
			idx := strings.Index(s[pos:], part)
			if idx < 0 {
				return false
			}
			if i == 0 && idx != 0 && !strings.HasPrefix(pattern, "*") {
				// Leading part must match prefix
				return false
			}
			pos += idx + len(part)
		}
		return true
	}

	return false
}
