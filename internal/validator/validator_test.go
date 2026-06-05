package validator

import (
	"strings"
	"testing"
)

func TestQuotePG(t *testing.T) {
	if quotePG("table") != `"table"` {
		t.Error("should double-quote PG identifier")
	}
	if quotePG(`ta"ble`) != `"ta""ble"` {
		t.Error("should escape double quotes")
	}
}

func TestQuoteMySQL(t *testing.T) {
	if quoteMySQL("table") != "`table`" {
		t.Error("should backtick-quote MySQL identifier")
	}
	if quoteMySQL("ta`ble") != "`ta``ble`" {
		t.Error("should escape backticks")
	}
}

func TestNormalizeDecimalString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"10.50", "10.5"},
		{"10.00", "10"},
		{"10.5", "10.5"},
		{"10", "10"},          // no decimal point, unchanged
		{"-3.1400", "-3.14"},
		{"0.00", "0"},
		{"hello", "hello"},   // not a decimal, unchanged
		{"1.0e5", "1.0e5"},   // scientific notation, not matched by decimalRe
		{"100", "100"},        // integer, unchanged
	}
	for _, tt := range tests {
		result := normalizeDecimalString(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeDecimalString(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestNormalizeString_LineEndings(t *testing.T) {
	// \r\n should be normalized to \n
	input := "2024年1月18日公告:\r\n 根据《上海康德莱控股集团有限公"
	output := normalizeString(input)
	if strings.Contains(output, "\r") {
		t.Errorf("normalizeString should remove \\r: got %q", output)
	}
	if !strings.Contains(output, "\n") {
		t.Errorf("normalizeString should preserve \\n: got %q", output)
	}

	// standalone \r should also be normalized
	input2 := "text with\r carriage return"
	output2 := normalizeString(input2)
	if strings.Contains(output2, "\r") {
		t.Errorf("normalizeString should remove standalone \\r: got %q", output2)
	}

	// No \r should be unchanged
	input3 := "plain text\nwith newlines"
	output3 := normalizeString(input3)
	if output3 != input3 {
		t.Errorf("normalizeString should not change text without \\r: got %q want %q", output3, input3)
	}
}

func TestTrimTrailingWhitespace(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello   ", "hello"},
		{"hello\t", "hello"},
		{"hello\r\n", "hello"},
		{"hello\n", "hello"},
		{"hello\r", "hello"},
		{"hello \t\n\r", "hello"},
		{"hello", "hello"},
		{"  hello  ", "  hello"},
		{"", ""},
	}
	for _, tt := range tests {
		result := trimTrailingWhitespace(tt.input)
		if result != tt.expected {
			t.Errorf("trimTrailingWhitespace(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestNormalizeTimestampString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"2024-01-01 12:30:00", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00.000000", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00.123456", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00.999", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00Z", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00+08:00", "2024-01-01 12:30:00"},
		{"2024-01-01 12:30:00.000000+08:00", "2024-01-01 12:30:00"},
		{"hello", "hello"},
		{"12345", "12345"},
	}
	for _, tt := range tests {
		result := normalizeTimestampString(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeTimestampString(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
