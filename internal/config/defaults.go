package config

import "time"

func DefaultConfig() *Config {
	return &Config{
		NATS: NATSConfig{
			URL:            "nats://nats.wasmcloud-system:4222",
			ConnectionName: "nats-jetstream-kv-resp",
			MaxReconnects:  -1,
			ReconnectWait:  Duration(2 * time.Second),
		},
		Server: ServerConfig{
			ListenAddr:     ":6380",
			MaxConnections: 1024,
			MaxArgs:        1024,
			MaxBulkLen:     512 * 1024 * 1024,
		},
		Buckets: []BucketConfig{
			{Name: "performers"},
		},
		TTL: TTLConfig{
			Enabled:        true,
			ReaperInterval: Duration(1 * time.Second),
		},
		Observability: ObservabilityConfig{
			Metrics: MetricsConfig{
				Enabled: true,
				Listen:  ":9090",
				Path:    "/metrics",
			},
			Health: HealthConfig{
				Enabled:       true,
				Listen:        ":6381",
				LivenessPath:  "/healthz",
				ReadinessPath: "/readyz",
			},
			Logging: LoggingConfig{
				Level:  "info",
				Format: "json",
			},
		},
	}
}
