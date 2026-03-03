// Package bridge provides an embeddable RESP-to-NATS-KV bridge.
package bridge

import (
	"context"
	"fmt"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/command"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/config"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/metrics"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/server"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/ttl"
	"github.com/gftdcojp/nats-jetstream-kv-resp/pkg/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Bridge is the embeddable RESP-to-NATS-KV bridge.
type Bridge struct {
	cfg    *config.Config
	nc     *nats.Conn
	ownNC  bool // true if we created the connection
	logger *zap.Logger
}

// New creates a new Bridge from a config.
func New(cfg *config.Config, logger *zap.Logger) *Bridge {
	return &Bridge{cfg: cfg, logger: logger}
}

// NewWithConn creates a Bridge with an existing NATS connection.
func NewWithConn(cfg *config.Config, nc *nats.Conn, logger *zap.Logger) *Bridge {
	return &Bridge{cfg: cfg, nc: nc, logger: logger}
}

// Run starts the bridge and blocks until ctx is cancelled.
func (b *Bridge) Run(ctx context.Context) error {
	// Connect to NATS if not provided
	if b.nc == nil {
		nc, err := natsutil.Connect(b.cfg.NATS, b.logger.Named("nats"))
		if err != nil {
			return fmt.Errorf("connecting to NATS: %w", err)
		}
		b.nc = nc
		b.ownNC = true
	}

	js, err := jetstream.New(b.nc)
	if err != nil {
		return fmt.Errorf("creating JetStream context: %w", err)
	}

	// Build bucket names
	bucketNames := make([]string, len(b.cfg.Buckets))
	for i, bc := range b.cfg.Buckets {
		bucketNames[i] = bc.Name
	}

	// Create backend
	backend, err := store.NewNATSBackend(b.nc, js, bucketNames, b.cfg.Server.ReadTimeout.Duration(), b.logger.Named("store"))
	if err != nil {
		return fmt.Errorf("creating NATS backend: %w", err)
	}

	// Create TTL tracker
	var tracker *ttl.Tracker
	var reaper *ttl.Reaper
	if b.cfg.TTL.Enabled {
		tracker = ttl.NewTracker(b.logger.Named("ttl"))
		tracker.LoadFromBackend(ctx, backend)
		reaper = ttl.NewReaper(tracker, backend, b.cfg.TTL.ReaperInterval.Duration(), b.logger.Named("ttl-reaper"))
	}

	// Create command registry
	registry := command.NewRegistry()

	// Create TCP server
	srv := server.New(server.Config{
		Addr:           b.cfg.Server.ListenAddr,
		Backend:        backend,
		Registry:       registry,
		TTLTracker:     tracker,
		MaxConnections: b.cfg.Server.MaxConnections,
		MaxArgs:        b.cfg.Server.MaxArgs,
		MaxBulkLen:     b.cfg.Server.MaxBulkLen,
		Logger:         b.logger.Named("server"),
	})

	g, gctx := errgroup.WithContext(ctx)

	// TCP server
	g.Go(func() error { return srv.Run(gctx) })

	// TTL reaper
	if reaper != nil {
		g.Go(func() error { return reaper.Run(gctx) })
	}

	// Health server
	if b.cfg.Observability.Health.Enabled {
		checker := metrics.NewHealthChecker(b.nc)
		g.Go(func() error { return metrics.RunHealthServer(gctx, b.cfg.Observability.Health, checker) })
	}

	// Metrics server
	if b.cfg.Observability.Metrics.Enabled {
		g.Go(func() error { return metrics.RunServer(gctx, b.cfg.Observability.Metrics) })
	}

	b.logger.Info("bridge started",
		zap.String("listen", b.cfg.Server.ListenAddr),
		zap.Int("buckets", len(b.cfg.Buckets)),
	)

	return g.Wait()
}

// Close gracefully shuts down the bridge.
func (b *Bridge) Close() {
	if b.ownNC && b.nc != nil {
		b.nc.Close()
	}
}
