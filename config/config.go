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
	// Emit selects the output emission policy: "all" persists and webhooks
	// every result (the default); "interesting" emits only non-approve
	// verdicts and rule failures, counting plain approvals in metrics —
	// the high-throughput setting.
	Emit string `yaml:"emit"`
	// RoutingField names the payload field used as the routing key
	// (default "entity_id"); events without it route by event ID.
	RoutingField string `yaml:"routing_field"`
	// ClusterPeers lists every peer's base URL (including this process's
	// own, identified by ClusterSelf). Empty means single-node. Ownership
	// of a routing key is decided by rendezvous hashing over this list, so
	// all peers must share the same list.
	ClusterPeers []string `yaml:"cluster_peers"`
	ClusterSelf  string   `yaml:"cluster_self"`
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
	if cfg.Emit == "" {
		cfg.Emit = "all"
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
		// valid
	default:
		return nil, fmt.Errorf("invalid log_level %q (must be debug, info, warn, or error)", cfg.LogLevel)
	}

	switch cfg.Emit {
	case "all", "interesting":
		// valid
	default:
		return nil, fmt.Errorf("invalid emit %q (must be all or interesting)", cfg.Emit)
	}

	if cfg.RoutingField == "" {
		cfg.RoutingField = "entity_id"
	}
	if len(cfg.ClusterPeers) > 0 && cfg.ClusterSelf == "" {
		return nil, fmt.Errorf("cluster_self is required when cluster_peers is set")
	}

	return cfg, nil
}
