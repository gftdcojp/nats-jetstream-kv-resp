package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NATS          NATSConfig          `yaml:"nats"`
	Server        ServerConfig        `yaml:"server"`
	Buckets       []BucketConfig      `yaml:"buckets"`
	TTL           TTLConfig           `yaml:"ttl"`
	Auth          AuthConfig          `yaml:"auth"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type NATSConfig struct {
	URL            string    `yaml:"url"`
	CredentialsFile string   `yaml:"credentials_file"`
	NKeySeedFile   string    `yaml:"nkey_seed_file"`
	TLS            TLSConfig `yaml:"tls"`
	ConnectionName string    `yaml:"connection_name"`
	MaxReconnects  int       `yaml:"max_reconnects"`
	ReconnectWait  Duration  `yaml:"reconnect_wait"`
}

type TLSConfig struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type ServerConfig struct {
	ListenAddr     string `yaml:"listen_addr"`
	MaxConnections int    `yaml:"max_connections"`
	ReadTimeout    Duration `yaml:"read_timeout"`
	WriteTimeout   Duration `yaml:"write_timeout"`
	MaxArgs        int    `yaml:"max_args"`
	MaxBulkLen     int64  `yaml:"max_bulk_len"`
}

type BucketConfig struct {
	Name   string `yaml:"name"`
	Create bool   `yaml:"create"`
}

type TTLConfig struct {
	Enabled        bool     `yaml:"enabled"`
	ReaperInterval Duration `yaml:"reaper_interval"`
}

type AuthConfig struct {
	Password string `yaml:"password"`
}

type ObservabilityConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
	Logging LoggingConfig `yaml:"logging"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
	Path    string `yaml:"path"`
}

type HealthConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Listen        string `yaml:"listen"`
	LivenessPath  string `yaml:"liveness_path"`
	ReadinessPath string `yaml:"readiness_path"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if c.NATS.URL == "" {
		return fmt.Errorf("nats.url is required")
	}
	if len(c.Buckets) == 0 {
		return fmt.Errorf("at least one bucket must be configured")
	}
	for i, b := range c.Buckets {
		if b.Name == "" {
			return fmt.Errorf("buckets[%d].name is required", i)
		}
	}
	return nil
}

// Duration wraps time.Duration for YAML unmarshaling of strings like "5m", "24h".
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}
