package config

import (
	"os"
	"runtime"
	"strings"
	"testing"
)

// TestLoad_Defaults verifies that calling Load with an empty path returns all
// expected default values.
func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") returned unexpected error: %v", err)
	}
	if cfg.Address != ":8080" {
		t.Errorf("Address: got %q, want %q", cfg.Address, ":8080")
	}
	if cfg.RulesDir != "./rules" {
		t.Errorf("RulesDir: got %q, want %q", cfg.RulesDir, "./rules")
	}
	if cfg.DuckDBPath != "fruitfly.duckdb" {
		t.Errorf("DuckDBPath: got %q, want %q", cfg.DuckDBPath, "fruitfly.duckdb")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.Workers != runtime.NumCPU() {
		t.Errorf("Workers: got %d, want %d", cfg.Workers, runtime.NumCPU())
	}
}

// TestLoad_ValidYAML verifies that values written to a temp YAML file are
// correctly loaded and override the defaults.
func TestLoad_ValidYAML(t *testing.T) {
	content := `
address: ":9090"
rules_dir: "/tmp/rules"
duckdb_path: "/tmp/test.duckdb"
webhook_url: "http://example.com/hook"
log_level: "debug"
workers: 4
`
	f, err := os.CreateTemp("", "fruitfly-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Address != ":9090" {
		t.Errorf("Address: got %q, want %q", cfg.Address, ":9090")
	}
	if cfg.RulesDir != "/tmp/rules" {
		t.Errorf("RulesDir: got %q, want %q", cfg.RulesDir, "/tmp/rules")
	}
	if cfg.DuckDBPath != "/tmp/test.duckdb" {
		t.Errorf("DuckDBPath: got %q, want %q", cfg.DuckDBPath, "/tmp/test.duckdb")
	}
	if cfg.WebhookURL != "http://example.com/hook" {
		t.Errorf("WebhookURL: got %q, want %q", cfg.WebhookURL, "http://example.com/hook")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.Workers != 4 {
		t.Errorf("Workers: got %d, want 4", cfg.Workers)
	}
}

// TestLoad_InvalidYAML verifies that a syntactically invalid YAML file causes
// Load to return an error.
func TestLoad_InvalidYAML(t *testing.T) {
	f, err := os.CreateTemp("", "fruitfly-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	if _, err := f.WriteString("[[["); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("Load with invalid YAML expected an error, got nil")
	}
}

// TestLoad_MissingFile verifies that a non-existent path returns defaults
// rather than an error, matching the documented behaviour.
func TestLoad_MissingFile(t *testing.T) {
	cfg, err := Load("/nonexistent/path.yaml")
	if err != nil {
		t.Fatalf("Load with missing file returned unexpected error: %v", err)
	}
	// At minimum the defaults should have been applied.
	if cfg.Address != ":8080" {
		t.Errorf("Address: got %q, want %q", cfg.Address, ":8080")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "info")
	}
}

// TestLoad_WorkersZero verifies that workers: 0 in the config file causes Load
// to fall back to runtime.NumCPU().
func TestLoad_WorkersZero(t *testing.T) {
	content := "workers: 0\n"
	f, err := os.CreateTemp("", "fruitfly-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Workers != runtime.NumCPU() {
		t.Errorf("Workers: got %d, want %d (runtime.NumCPU)", cfg.Workers, runtime.NumCPU())
	}
}

// TestLoad_InvalidLogLevel verifies that an unrecognised log_level value causes
// Load to return an error containing "invalid log_level".
func TestLoad_InvalidLogLevel(t *testing.T) {
	content := "log_level: \"invalid\"\n"
	f, err := os.CreateTemp("", "fruitfly-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("Load with invalid log_level expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid log_level") {
		t.Errorf("error message %q does not contain %q", err.Error(), "invalid log_level")
	}
}

// TestLoad_PartialYAML verifies that a YAML file with only one field set leaves
// all other fields at their default values.
func TestLoad_PartialYAML(t *testing.T) {
	content := "address: \":9090\"\n"
	f, err := os.CreateTemp("", "fruitfly-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if cfg.Address != ":9090" {
		t.Errorf("Address: got %q, want %q", cfg.Address, ":9090")
	}
	if cfg.RulesDir != "./rules" {
		t.Errorf("RulesDir: got %q, want %q", cfg.RulesDir, "./rules")
	}
	if cfg.DuckDBPath != "fruitfly.duckdb" {
		t.Errorf("DuckDBPath: got %q, want %q", cfg.DuckDBPath, "fruitfly.duckdb")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.Workers != runtime.NumCPU() {
		t.Errorf("Workers: got %d, want %d (runtime.NumCPU)", cfg.Workers, runtime.NumCPU())
	}
}
