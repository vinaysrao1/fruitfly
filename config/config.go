package config

import (
	"fmt"
	"os"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config holds the fruitfly runtime configuration.
type Config struct {
	Address    string `yaml:"address"`
	RulesDir   string `yaml:"rules_dir"`
	DuckDBPath string `yaml:"duckdb_path"`
	WebhookURL string `yaml:"webhook_url"`
	Workers    int    `yaml:"workers"`
	LogLevel   string `yaml:"log_level"`
}

// Load reads a YAML config file and returns a Config with defaults applied.
// If path is empty or the file does not exist, defaults are returned.
func Load(path string) (*Config, error) {
	cfg := &Config{}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("read config %s: %w", path, err)
			}
			// File not found: use defaults.
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config %s: %w", path, err)
			}
		}
	}

	// Apply defaults for missing values.
	if cfg.Address == "" {
		cfg.Address = ":8080"
	}
	if cfg.RulesDir == "" {
		cfg.RulesDir = "./rules"
	}
	if cfg.DuckDBPath == "" {
		cfg.DuckDBPath = "fruitfly.duckdb"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
		// valid
	default:
		return nil, fmt.Errorf("invalid log_level %q (must be debug, info, warn, or error)", cfg.LogLevel)
	}

	return cfg, nil
}
