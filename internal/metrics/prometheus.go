package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ConnectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resp_bridge_connections_total",
		Help: "Total client connections accepted",
	})

	ConnectionsCurrent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "resp_bridge_connections_current",
		Help: "Current active client connections",
	})

	CommandsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resp_bridge_commands_total",
		Help: "Total commands processed",
	}, []string{"command", "status"})

	CommandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "resp_bridge_command_duration_seconds",
		Help:    "Command processing latency",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
	}, []string{"command"})

	NATSOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resp_bridge_nats_ops_total",
		Help: "Total NATS KV operations",
	}, []string{"operation", "bucket", "status"})

	TTLExpirationsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resp_bridge_ttl_expirations_total",
		Help: "Total keys expired by TTL reaper",
	})
)

// RunServer starts the Prometheus metrics HTTP server.
func RunServer(ctx context.Context, cfg config.MetricsConfig) error {
	mux := http.NewServeMux()
	path := cfg.Path
	if path == "" {
		path = "/metrics"
	}
	mux.Handle(path, promhttp.Handler())

	srv := &http.Server{Addr: cfg.Listen, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
