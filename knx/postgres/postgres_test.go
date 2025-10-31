package postgres

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig("postgres://user:pass@localhost:6543/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.host != "localhost" {
		t.Fatalf("host = %q, want localhost", cfg.host)
	}
	if cfg.port != "6543" {
		t.Fatalf("port = %q, want 6543", cfg.port)
	}
	if cfg.user != "user" || cfg.password != "pass" {
		t.Fatalf("credentials mismatch: %#v", cfg)
	}
	if cfg.database != "db" {
		t.Fatalf("database = %q, want db", cfg.database)
	}
	if cfg.sslMode != "disable" {
		t.Fatalf("sslMode = %q, want disable", cfg.sslMode)
	}
}

func TestParseConfigAppliesFallbacks(t *testing.T) {
	cfg, err := parseConfig("postgres://user@/?sslmode=disable")
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.host != "127.0.0.1" {
		t.Fatalf("default host = %q, want 127.0.0.1", cfg.host)
	}
	if cfg.port != "5432" {
		t.Fatalf("default port = %q, want 5432", cfg.port)
	}
	if cfg.database != "user" {
		t.Fatalf("default database = %q, want user", cfg.database)
	}
}

func TestParseConfigErrors(t *testing.T) {
	if _, err := parseConfig("http://user:pass@host/db"); err == nil {
		t.Fatal("parseConfig() expected error for unsupported scheme")
	}
	if _, err := parseConfig("postgres://host/db"); err == nil {
		t.Fatal("parseConfig() expected error for missing user")
	}
	if _, err := parseConfig("postgres://user:pass@host/db?sslmode=require"); err == nil {
		t.Fatal("parseConfig() expected error for sslmode")
	}
}

func TestParseRowDescription(t *testing.T) {
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, uint16(2)); err != nil {
		t.Fatalf("binary.Write() error = %v", err)
	}
	payload.WriteString("col1")
	payload.WriteByte(0)
	payload.Write(make([]byte, 18))
	payload.WriteString("col2")
	payload.WriteByte(0)
	payload.Write(make([]byte, 18))

	cols, err := parseRowDescription(payload.Bytes())
	if err != nil {
		t.Fatalf("parseRowDescription() error = %v", err)
	}
	if len(cols) != 2 || cols[0] != "col1" || cols[1] != "col2" {
		t.Fatalf("parseRowDescription() = %v", cols)
	}
}

func TestParseDataRow(t *testing.T) {
	columns := []string{"first", "second"}
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, uint16(len(columns))); err != nil {
		t.Fatalf("binary.Write() error = %v", err)
	}
	if err := binary.Write(&payload, binary.BigEndian, int32(3)); err != nil {
		t.Fatalf("binary.Write() error = %v", err)
	}
	payload.WriteString("foo")
	if err := binary.Write(&payload, binary.BigEndian, int32(-1)); err != nil {
		t.Fatalf("binary.Write() error = %v", err)
	}

	row, err := parseDataRow(payload.Bytes(), columns)
	if err != nil {
		t.Fatalf("parseDataRow() error = %v", err)
	}
	if got := string(row["first"]); got != "foo" {
		t.Fatalf("row[first] = %q, want foo", got)
	}
	if row["second"] != nil {
		t.Fatalf("row[second] = %v, want nil", row["second"])
	}

	if _, err := parseDataRow(payload.Bytes(), columns[:1]); err == nil {
		t.Fatal("parseDataRow() expected error for column mismatch")
	}
}

func TestQuoteHelpers(t *testing.T) {
	if got := quoteLiteral("a'b"); got != "'a''b'" {
		t.Fatalf("quoteLiteral() = %q", got)
	}
	if got := nullableLiteral(" "); got != "NULL" {
		t.Fatalf("nullableLiteral() = %q, want NULL", got)
	}
	if got := nullableLiteral("value"); got != "'value'" {
		t.Fatalf("nullableLiteral() = %q, want 'value'", got)
	}
}
