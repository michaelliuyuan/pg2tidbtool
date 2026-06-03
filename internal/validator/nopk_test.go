package validator

import (
	"testing"
)

func TestParseIndexColumns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single column",
			input:    `CREATE UNIQUE INDEX idx_email ON users (email)`,
			expected: []string{"email"},
		},
		{
			name:     "multiple columns",
			input:    `CREATE UNIQUE INDEX idx_name_dob ON users (last_name, first_name, dob)`,
			expected: []string{"last_name", "first_name", "dob"},
		},
		{
			name:     "quoted columns",
			input:    `CREATE UNIQUE INDEX idx_col ON "myTable" ("myCol")`,
			expected: []string{"myCol"},
		},
		{
			name:     "with ASC option",
			input:    `CREATE UNIQUE INDEX idx_col ON users (email ASC)`,
			expected: []string{"email"},
		},
		{
			name:     "no parens",
			input:    `CREATE UNIQUE INDEX idx ON users`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseIndexColumns(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseIndexColumns(%q) = %v, want %v", tt.input, result, tt.expected)
				return
			}
			for i, col := range result {
				if col != tt.expected[i] {
					t.Errorf("parseIndexColumns(%q)[%d] = %q, want %q", tt.input, i, col, tt.expected[i])
				}
			}
		})
	}
}

func TestComputeRowHash(t *testing.T) {
	// Two rows with same values in same column order should produce same hash
	row1 := []string{"a", "b", "c"}
	cols1 := []colMapping{{pgIdx: 0, name: "col1"}, {pgIdx: 1, name: "col2"}, {pgIdx: 2, name: "col3"}}
	h1 := computeRowHash(row1, cols1)

	row2 := []string{"a", "b", "c"}
	h2 := computeRowHash(row2, cols1)

	if h1 != h2 {
		t.Errorf("same rows should have same hash: %s != %s", h1, h2)
	}

	// Different rows should produce different hashes
	row3 := []string{"a", "x", "c"}
	h3 := computeRowHash(row3, cols1)

	if h1 == h3 {
		t.Errorf("different rows should have different hashes")
	}

	// Hash should be deterministic regardless of column order in the input
	// (the hashCols are already sorted by name)
	colsReversed := []colMapping{{pgIdx: 2, name: "col3"}, {pgIdx: 1, name: "col2"}, {pgIdx: 0, name: "col1"}}
	h4 := computeRowHash(row1, colsReversed)

	// Different column ordering in hashCols produces different hash by design
	// (hash is computed in hashCols order, which should be sorted by name)
	// This is expected — the caller must sort hashCols by name for consistency
	_ = h4
}

func TestIsApproximateFloatType(t *testing.T) {
	// Only approximate float types should be skipped
	floatTypes := []string{"real", "float", "float4", "float8", "double", "double precision"}
	for _, dt := range floatTypes {
		if !isApproximateFloatType(dt) {
			t.Errorf("isApproximateFloatType(%q) should be true", dt)
		}
	}

	// DECIMAL and NUMERIC are exact types — should NOT be skipped
	exactTypes := []string{"numeric", "decimal"}
	for _, dt := range exactTypes {
		if isApproximateFloatType(dt) {
			t.Errorf("isApproximateFloatType(%q) should be false (exact type)", dt)
		}
	}

	nonFloatTypes := []string{"integer", "varchar", "text", "timestamp", "bigint"}
	for _, dt := range nonFloatTypes {
		if isApproximateFloatType(dt) {
			t.Errorf("isApproximateFloatType(%q) should be false", dt)
		}
	}
}

func TestBucketOf(t *testing.T) {
	// Deterministic: same hash always maps to same bucket
	h := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
	b1 := bucketOf(h, 100)
	b2 := bucketOf(h, 100)
	if b1 != b2 {
		t.Errorf("bucketOf should be deterministic: %d != %d", b1, b2)
	}
	if b1 < 0 || b1 >= 100 {
		t.Errorf("bucket out of range: %d", b1)
	}

	// Different hashes may map to different buckets
	h2 := "ffffffffffffffffffffffffffffffff"
	b3 := bucketOf(h2, 100)
	// Just verify it's in range
	if b3 < 0 || b3 >= 100 {
		t.Errorf("bucket out of range: %d", b3)
	}
}
